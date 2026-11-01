package Mojo::Centrifuge;
use strict;
use warnings;
use Carp ();
use Scalar::Util qw(blessed weaken);
use Mojo::Centrifuge::Util qw(encode_json decode_json uuid TRUE FALSE bus_name str);
use Mojo::Centrifuge::Namespaces;
use Mojo::Centrifuge::JWT;
use Mojo::Centrifuge::Engine::Memory;
use Mojo::Centrifuge::Client;

our $VERSION = '1.0.0';

my %KNOWN = map { $_ => 1 } qw(
    engine namespaces secret jwt api_key node_id name log scheduler now
    ping_interval pong_timeout connect_timeout expired_close_delay presence_ttl
    message_size_limit client_channel_limit allow_anonymous hooks hook_timeout
    channel_max_length history_meta_ttl
);
my %HOOKS = map { $_ => 1 } qw(connect subscribe publish rpc refresh);

my %ERR = (
    internal      => [100, 'internal server error'],
    unknown       => [102, 'unknown channel'],
    method        => [104, 'method not found'],
    bad_request   => [107, 'bad request'],
    not_available => [108, 'not available'],
    unrecoverable => [112, 'unrecoverable position'],
);

for my $f (qw(engine namespaces jwt api_key node_id name scheduler ping_interval pong_timeout
              connect_timeout expired_close_delay presence_ttl message_size_limit
              client_channel_limit allow_anonymous hook_timeout)) {
    no strict 'refs';
    *{$f} = sub { $_[0]{$f} };
}
sub version { $VERSION }
sub now     { $_[0]{now}->() }

sub new {
    my ($class, %a) = @_;
    for my $k (keys %a) { Carp::croak("unknown option '$k'") unless $KNOWN{$k} }

    my $sched = $a{scheduler} || do {
        require Mojo::Centrifuge::Scheduler::IOLoop;
        Mojo::Centrifuge::Scheduler::IOLoop->new;
    };
    my $now = $a{now} || ($sched->can('now') ? sub { $sched->now } : sub { time });

    my $ns = $a{namespaces} || {};
    $ns = Mojo::Centrifuge::Namespaces->new(
        %$ns,
        (defined $a{channel_max_length} ? (channel_max_length => $a{channel_max_length}) : ()),
        (defined $a{history_meta_ttl}   ? (history_meta_ttl   => $a{history_meta_ttl})   : ()),
    ) unless blessed $ns;

    my $jwt = $a{jwt} || (defined $a{secret} && length $a{secret}
        ? Mojo::Centrifuge::JWT->new(secret => $a{secret}, now => $now) : undef);

    my $hooks = $a{hooks} || {};
    for my $h (keys %$hooks) {
        Carp::croak("unknown hook '$h'") unless $HOOKS{$h};
        Carp::croak("hook '$h' must be a code ref") unless ref $hooks->{$h} eq 'CODE';
    }

    my $self = bless {
        engine               => $a{engine} || Mojo::Centrifuge::Engine::Memory->new(now => $now),
        namespaces           => $ns,
        jwt                  => $jwt,
        api_key              => $a{api_key},
        node_id              => $a{node_id} // uuid(),
        name                 => $a{name} // 'node',
        log                  => $a{log},
        scheduler            => $sched,
        now                  => $now,
        ping_interval        => $a{ping_interval}        // 25,
        pong_timeout         => $a{pong_timeout}         // 10,
        connect_timeout      => $a{connect_timeout}      // 10,
        expired_close_delay  => $a{expired_close_delay}  // 25,
        presence_ttl         => $a{presence_ttl}         // 60,
        message_size_limit   => $a{message_size_limit}   // 65536,
        client_channel_limit => $a{client_channel_limit} // 128,
        allow_anonymous      => $a{allow_anonymous}      // 0,
        hook_timeout         => $a{hook_timeout}         // 10,
        hooks                => $hooks,
        clients              => {},    # client id => Client
        users                => {},    # user => {client id => Client}
        channels             => {},    # channel => {client id => Client}
        listening            => {},
        started              => 0,
    }, $class;
    return $self;
}

sub hook { $_[0]{hooks}{ $_[1] } }

# ---- logging --------------------------------------------------------------
sub _log {
    my ($self, $level, $msg) = @_;
    my $log = $self->{log} or return;
    eval { $log->$level($msg); 1 };
}
sub log_info  { shift->_log('info',  @_) }
sub log_warn  { shift->_log('warn',  @_) }
sub log_error { shift->_log('error', @_) }

# ---- lifecycle ------------------------------------------------------------
sub start {
    my $self = shift;
    return $self if $self->{started}++;
    $self->{started_at} = $self->now;
    my $w = $self;
    weaken $w;
    $self->{engine}->bus_subscribe(Mojo::Centrifuge::Util::CONTROL(), sub { $w->_on_bus(@_) if $w }, $self);
    my $every = $self->{presence_ttl} / 3;
    $every = 1 if $every < 1;
    $self->{hb_timer} = $self->{scheduler}->recurring($every, sub { $w->_presence_heartbeat if $w });
    return $self;
}

sub stop {
    my $self = shift;
    return unless $self->{started};
    $self->{started} = 0;
    $self->{scheduler}->remove(delete $self->{hb_timer});
    my @clients = values %{ $self->{clients} };
    $_->disconnect(3001, 'shutdown') for @clients;
    $self->{engine}->bus_unsubscribe(Mojo::Centrifuge::Util::CONTROL(), $self);
}

# transport glue: send => sub($text), close => sub($code, $reason_json)
sub new_client {
    my ($self, %a) = @_;
    $self->start;
    return Mojo::Centrifuge::Client->new(hub => $self, %a);
}

# ---- registry -------------------------------------------------------------
sub register {
    my ($self, $c) = @_;
    $self->{clients}{ $c->id } = $c;
    $self->{users}{ $c->user }{ $c->id } = $c;
}

sub unregister {
    my ($self, $c) = @_;
    delete $self->{clients}{ $c->id };
    my $set = $self->{users}{ $c->user } or return;
    delete $set->{ $c->id };
    delete $self->{users}{ $c->user } unless keys %$set;
}

sub add_sub {
    my ($self, $client, $ch) = @_;
    $self->{channels}{$ch}{ $client->id } = $client;
    return if $self->{listening}{$ch}++;
    my $w = $self;
    weaken $w;
    $self->{engine}->bus_subscribe(bus_name($ch), sub { $w->_on_bus(@_) if $w }, $self);
}

sub remove_sub {
    my ($self, $client, $ch) = @_;
    my $set = $self->{channels}{$ch} or return;
    delete $set->{ $client->id };
    return if keys %$set;
    delete $self->{channels}{$ch};
    if (delete $self->{listening}{$ch}) {
        eval { $self->{engine}->bus_unsubscribe(bus_name($ch), $self); 1 };
    }
}

# ---- bus ------------------------------------------------------------------
sub _on_bus {
    my ($self, $name, $json) = @_;
    my $m = eval { decode_json($json) };
    return unless ref $m eq 'HASH';
    my $t = $m->{t} // '';
    if    ($t eq 'pub')                       { $self->_deliver_pub($m) }
    elsif ($t eq 'join' || $t eq 'leave')     { $self->_deliver_presence($m) }
    elsif ($t eq 'disconnect')                { $self->_apply_disconnect($m) unless ($m->{node} // '') eq $self->{node_id} }
    elsif ($t eq 'unsub')                     { $self->_apply_unsub($m)      unless ($m->{node} // '') eq $self->{node_id} }
}

sub _deliver_pub {
    my ($self, $m) = @_;
    my $ch  = $m->{ch};
    return unless defined $ch && !ref $ch;
    my $set = $self->{channels}{$ch} or return;
    my %pub = (data => $m->{d}, (($m->{o} // 0) > 0 ? (offset => $m->{o} + 0) : ()));
    $pub{info} = $m->{i} if ref $m->{i} eq 'HASH';
    my $frame = encode_json({ push => { channel => $ch, pub => \%pub } });
    my @clients = values %$set;
    $_->deliver_pub($ch, ($m->{o} // 0) + 0, $m->{e} // '', $frame) for @clients;
}

sub _deliver_presence {
    my ($self, $m) = @_;
    my $ch  = $m->{ch};
    return unless defined $ch && !ref $ch && ref $m->{info} eq 'HASH';
    my $set = $self->{channels}{$ch} or return;
    my $frame = encode_json({ push => { channel => $ch, $m->{t} => { info => $m->{info} } } });
    my @clients = grep { $_->id ne ($m->{client} // '') } values %$set;
    $_->deliver_event($ch, $frame) for @clients;
}

sub emit_presence {
    my ($self, $type, $ch, $info) = @_;
    $self->{engine}->bus_publish(bus_name($ch),
        encode_json({ t => $type, ch => str($ch), client => $info->{client}, info => $info }));
}

# ---- publish / history / presence ----------------------------------------
# Returns {offset, epoch} or (undef, $error_key)
sub publish {
    my ($self, $ch, $data, %o) = @_;
    my ($opts, $why) = $self->{namespaces}->resolve($ch);
    return (undef, $why) unless $opts;
    my $dj = encode_json($data);
    my $ij = ref $o{info} eq 'HASH' ? encode_json($o{info}) : 'null';
    my $h  = ($o{skip_history} || !$opts->{history}) ? {} : $self->{namespaces}->history_opts($opts);
    my ($offset, $epoch) = $self->{engine}->publish($ch, $dj, $ij, $h);
    return { offset => $offset + 0, epoch => $epoch };
}

sub stream_position {
    my ($self, $ch, $opts) = @_;
    return $self->{engine}->stream_info($ch, $self->{namespaces}->history_opts($opts));
}

# Recovery for a client that was at ($offset, $epoch).
# Returns ($recovered, \@publications, $top_offset, $epoch)
sub recover {
    my ($self, $ch, $opts, $since_offset, $since_epoch) = @_;
    $self->stream_position($ch, $opts);    # make sure the stream (and its epoch) exists
    my ($all, $top, $epoch) = $self->{engine}->history($ch);
    return (0, [], $top, $epoch) unless defined $since_epoch && $since_epoch eq $epoch;
    return (0, [], $top, $epoch) if $since_offset > $top;
    return (1, [], $top, $epoch) if $since_offset == $top;
    my @missed = grep { $_->{offset} > $since_offset } @$all;
    return (1, \@missed, $top, $epoch) if @missed == $top - $since_offset;
    return (0, [], $top, $epoch);
}

sub _pub_out {
    my $p = shift;
    return { data => $p->{data}, offset => $p->{offset} + 0, (ref $p->{info} eq 'HASH' ? (info => $p->{info}) : ()) };
}

# Returns {publications, epoch, offset} or (undef, $error_key)
sub history {
    my ($self, $ch, %o) = @_;
    my ($opts, $why) = $self->{namespaces}->resolve($ch);
    return (undef, $why) unless $opts;
    return (undef, 'not_available') unless $opts->{history};

    my ($all, $top, $epoch) = $self->{engine}->history($ch);
    my @items = @$all;
    my $since = $o{since};
    if (ref $since eq 'HASH') {
        my $so = $since->{offset} // 0;
        return (undef, 'bad_request') if ref $so || $so !~ /\A\d+\z/;
        return (undef, 'unrecoverable') if defined $since->{epoch} && length $since->{epoch} && $since->{epoch} ne $epoch;
        @items = $o{reverse} ? grep { $_->{offset} < $so } @items : grep { $_->{offset} > $so } @items;
    }
    @items = reverse @items if $o{reverse};
    my $limit = $o{limit} // 0;
    return (undef, 'bad_request') if ref $limit || $limit !~ /\A-?\d+\z/;
    @items = $limit < 0 ? @items : (@items > $limit ? @items[0 .. $limit - 1] : @items);
    @items = () if $limit == 0;
    return { publications => [ map { _pub_out($_) } @items ], epoch => $epoch, offset => $top + 0 };
}

sub history_remove {
    my ($self, $ch) = @_;
    my ($opts, $why) = $self->{namespaces}->resolve($ch);
    return (undef, $why) unless $opts;
    return (undef, 'not_available') unless $opts->{history};
    $self->{engine}->history_remove($ch);
    return {};
}

sub presence {
    my ($self, $ch) = @_;
    my ($opts, $why) = $self->{namespaces}->resolve($ch);
    return (undef, $why) unless $opts;
    return (undef, 'not_available') unless $opts->{presence};
    return $self->{engine}->presence($ch);
}

sub presence_stats {
    my ($self, $ch) = @_;
    my ($p, $err) = $self->presence($ch);
    return (undef, $err) unless $p;
    my %users = map { ($_->{user} // '') => 1 } values %$p;
    return { num_clients => scalar(keys %$p) + 0, num_users => scalar(keys %users) + 0 };
}

sub presence_add {
    my ($self, $ch, $info) = @_;
    $self->{engine}->presence_add($ch, $info->{client}, encode_json($info), $self->{presence_ttl});
}

sub presence_remove {
    my ($self, $ch, $client_id) = @_;
    $self->{engine}->presence_remove($ch, $client_id);
}

sub _presence_heartbeat {
    my $self = shift;
    for my $c (values %{ $self->{clients} }) {
        eval { $c->refresh_presence; 1 } or $self->log_warn("presence heartbeat failed: $@");
    }
}

# ---- user control (cluster-wide) -----------------------------------------
sub disconnect_user {
    my ($self, %o) = @_;
    my $ev = {
        t => 'disconnect', node => $self->{node_id}, user => str($o{user}),
        client => $o{client}, code => ($o{code} // 3000) + 0, reason => $o{reason} // 'disconnect',
        whitelist => $o{whitelist} || [],
    };
    $self->_apply_disconnect($ev);
    $self->{engine}->bus_publish(Mojo::Centrifuge::Util::CONTROL(), encode_json($ev));
}

sub _apply_disconnect {
    my ($self, $ev) = @_;
    my $set = $self->{users}{ $ev->{user} // '' } or return;
    my %keep = map { $_ => 1 } @{ $ev->{whitelist} || [] };
    my @clients = values %$set;
    for my $c (@clients) {
        next if defined $ev->{client} && length $ev->{client} && $c->id ne $ev->{client};
        next if $keep{ $c->id };
        $c->disconnect($ev->{code} // 3000, $ev->{reason} // 'disconnect');
    }
}

sub unsubscribe_user {
    my ($self, %o) = @_;
    my $ev = { t => 'unsub', node => $self->{node_id}, user => str($o{user}), channel => str($o{channel}), client => $o{client} };
    $self->_apply_unsub($ev);
    $self->{engine}->bus_publish(Mojo::Centrifuge::Util::CONTROL(), encode_json($ev));
}

sub _apply_unsub {
    my ($self, $ev) = @_;
    my $set = $self->{users}{ $ev->{user} // '' } or return;
    my @clients = values %$set;
    for my $c (@clients) {
        next if defined $ev->{client} && length $ev->{client} && $c->id ne $ev->{client};
        $c->server_unsubscribe($ev->{channel});
    }
}

# ---- introspection ------------------------------------------------------
sub channels {
    my ($self, $pattern) = @_;
    my $re;
    if (defined $pattern && length $pattern) {
        $re = quotemeta $pattern;
        $re =~ s/\\\*/.*/g;
        $re = qr/\A$re\z/s;
    }
    my %out;
    for my $ch (keys %{ $self->{channels} }) {
        next if $re && $ch !~ $re;
        $out{$ch} = { num_clients => scalar(keys %{ $self->{channels}{$ch} }) + 0 };
    }
    return { channels => \%out };
}

sub info {
    my $self = shift;
    return { nodes => [ {
        uid          => $self->{node_id},
        name         => $self->{name},
        version      => $VERSION,
        num_clients  => scalar(keys %{ $self->{clients} }) + 0,
        num_users    => scalar(keys %{ $self->{users} }) + 0,
        num_channels => scalar(keys %{ $self->{channels} }) + 0,
        uptime       => $self->{started_at} ? $self->now - $self->{started_at} : 0,
    } ] };
}

# ---- server API (shared by HTTP and tests) ---------------------------------
sub _need_channel { my $p = shift; return (defined $p->{channel} && !ref $p->{channel} && length $p->{channel}) }

my %API = (
    publish => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless _need_channel($p) && exists $p->{data};
        my ($r, $err) = $self->publish($p->{channel}, $p->{data}, skip_history => $p->{skip_history});
        return (undef, $err) unless $r;
        return $r->{offset} ? $r : {};
    },
    broadcast => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless ref $p->{channels} eq 'ARRAY' && @{ $p->{channels} } && exists $p->{data};
        my @replies;
        for my $ch (@{ $p->{channels} }) {
            my @r = eval { $self->publish($ch, $p->{data}, skip_history => $p->{skip_history}) };
            if ($@) { $self->log_error("broadcast: $@"); push @replies, { error => $self->error_obj('internal') } }
            elsif (!$r[0]) { push @replies, { error => $self->error_obj($r[1]) } }
            else { push @replies, { result => ($r[0]{offset} ? $r[0] : {}) } }
        }
        return { responses => \@replies };
    },
    unsubscribe => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless defined $p->{user} && !ref $p->{user} && _need_channel($p);
        $self->unsubscribe_user(user => $p->{user}, channel => $p->{channel}, client => $p->{client});
        return {};
    },
    disconnect => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless defined $p->{user} && !ref $p->{user};
        my $d = ref $p->{disconnect} eq 'HASH' ? $p->{disconnect} : {};
        $self->disconnect_user(
            user => $p->{user}, client => $p->{client}, whitelist => $p->{whitelist},
            code => $d->{code}, reason => $d->{reason},
        );
        return {};
    },
    presence => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless _need_channel($p);
        my ($r, $err) = $self->presence($p->{channel});
        return $r ? { presence => $r } : (undef, $err);
    },
    presence_stats => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless _need_channel($p);
        return $self->presence_stats($p->{channel});
    },
    history => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless _need_channel($p);
        return $self->history($p->{channel}, limit => $p->{limit}, since => $p->{since}, reverse => $p->{reverse});
    },
    history_remove => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless _need_channel($p);
        return $self->history_remove($p->{channel});
    },
    channels => sub { $_[0]->channels($_[1]{pattern}) },
    info     => sub { $_[0]->info },
    batch    => sub {
        my ($self, $p) = @_;
        return (undef, 'bad_request') unless ref $p->{commands} eq 'ARRAY';
        return { replies => [ map { ref $_ eq 'HASH' ? $self->api($_->{method}, $_->{params}) : { error => $self->error_obj('bad_request') } } @{ $p->{commands} } ] };
    },
);

sub error_obj {
    my ($self, $key) = @_;
    my $e = $ERR{ $key // 'internal' } || $ERR{internal};
    return { code => $e->[0], message => $e->[1] };
}

# Returns {result => ...} or {error => {code, message}}
sub api {
    my ($self, $method, $params) = @_;
    $params = {} unless ref $params eq 'HASH';
    my $fn = (defined $method && !ref $method) ? $API{$method} : undef;
    return { error => $self->error_obj('method') } unless $fn;
    my @r = eval { $fn->($self, $params) };
    if ($@) {
        $self->log_error("api $method failed: $@");
        return { error => $self->error_obj('internal') };
    }
    my ($res, $err) = @r;
    return { error => $self->error_obj($err) } if defined $err;
    return { result => $res // {} };
}

1;
