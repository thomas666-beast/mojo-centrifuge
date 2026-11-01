package Mojo::Centrifuge::Client;
use strict;
use warnings;
use Scalar::Util qw(weaken);
use Mojo::Centrifuge::Util qw(encode_json decode_json uuid TRUE FALSE str);

# One instance per connection. Speaks the Centrifugo JSON client protocol:
# commands {"id":N,"<method>":{...}} -> replies {"id":N,"<method>":{...}} or
# {"id":N,"error":{...}}, pushes {"push":{"channel":..,"pub":{..}}}, and an
# empty {} as ping/pong. Several commands may share one frame (newline separated).

my @METHODS = qw(connect subscribe unsubscribe publish presence presence_stats
                 history rpc refresh sub_refresh send);

sub new {
    my ($class, %a) = @_;
    my $hub  = $a{hub} or die "hub required\n";
    my $self = bless {
        hub        => $hub,
        send_cb    => $a{send},
        close_cb   => $a{close},
        transport  => $a{transport} // 'websocket',
        id         => uuid(),
        state      => 'new',      # new -> connecting -> connected -> closed
        closed     => 0,
        user       => '',
        conn_info  => undef,
        expire_at  => undef,
        subs       => {},
        pending    => {},         # channels being subscribed (async hooks)
        queue      => [],
        timers     => {},
        awaiting_pong => 0,
    }, $class;
    my $w = $self;
    weaken $w;
    $self->{timers}{connect} = $hub->scheduler->timer(
        $hub->connect_timeout, sub { $w->disconnect(3502, 'stale') if $w && $w->{state} eq 'new' });
    return $self;
}

sub id        { $_[0]{id} }
sub user      { $_[0]{user} }
sub state     { $_[0]{state} }
sub is_closed { $_[0]{closed} }
sub subscriptions { return sort keys %{ $_[0]{subs} } }

# ---- output ---------------------------------------------------------------
sub _send {
    my ($self, $text) = @_;
    return if $self->{closed};
    my $ok = eval { $self->{send_cb}->($text) if $self->{send_cb}; 1 };
    unless ($ok) {
        $self->{hub}->log_warn("client $self->{id}: send failed: $@");
        $self->_cleanup;
    }
}
sub _send_obj { $_[0]->_send(encode_json($_[1])) }

sub _reply {
    my ($self, $id, $name, $result) = @_;
    return unless defined $id;
    $self->_send_obj({ id => $id + 0, $name => $result // {} });
}

sub _err {
    my ($self, $id, $code, $message, $temporary) = @_;
    return unless defined $id;
    $self->_send_obj({ id => $id + 0, error => { code => $code + 0, message => $message, ($temporary ? (temporary => TRUE) : ()) } });
}

# ---- closing --------------------------------------------------------------
sub _reconnect_flag { my $c = shift; return ($c < 3500 || ($c >= 4000 && $c < 4500)) ? TRUE : FALSE }

sub disconnect {
    my ($self, $code, $reason) = @_;
    return if $self->{closed};
    $code //= 3000;
    $reason //= '';
    $self->_cleanup;
    my $why = encode_json({ reason => $reason, reconnect => _reconnect_flag($code) });
    eval { $self->{close_cb}->($code, $why) if $self->{close_cb}; 1 }
        or $self->{hub}->log_warn("client $self->{id}: close failed: $@");
}

# Call when the transport went away.
sub closed { $_[0]->_cleanup }

sub _cleanup {
    my $self = shift;
    return if $self->{closed}++;
    my $hub = $self->{hub};
    $self->{state} = 'closed';
    $hub->scheduler->remove(delete $self->{timers}{$_}) for keys %{ $self->{timers} };
    for my $ch (keys %{ $self->{subs} }) {
        eval { $self->_drop_sub($ch); 1 } or $hub->log_warn("client $self->{id}: cleanup of '$ch' failed: $@");
    }
    $hub->unregister($self) if $self->{registered};
    $self->{queue} = [];
}

# ---- input ----------------------------------------------------------------
sub _activity {
    my $self = shift;
    $self->{awaiting_pong} = 0;
    $self->{hub}->scheduler->remove(delete $self->{timers}{pong});
}

sub handle_frame {
    my ($self, $text) = @_;
    return if $self->{closed};
    return $self->disconnect(3501, 'bad request') unless defined $text && !ref $text;
    return $self->disconnect(3503, 'message size limit exceeded') if length $text > $self->{hub}->message_size_limit;
    $self->_activity;

    my @cmds;
    for my $line (split /\n/, $text) {
        next unless $line =~ /\S/;
        my $c = eval { decode_json($line) };
        return $self->disconnect(3501, 'bad request') unless ref $c eq 'HASH';
        push @cmds, $c;
    }
    for my $c (@cmds) {
        last if $self->{closed};
        $self->_dispatch($c);
    }
}

sub _dispatch {
    my ($self, $cmd) = @_;
    return unless %$cmd;    # empty command == pong, already handled by _activity

    my $id = $cmd->{id};
    return $self->disconnect(3501, 'bad request')
        if defined $id && (ref $id || $id !~ /\A\d+\z/);

    my ($method) = grep { exists $cmd->{$_} } @METHODS;
    if (!defined $method) {
        my @other = grep { $_ ne 'id' } keys %$cmd;
        return $self->_err($id, 104, 'method not found') if @other;
        # only {"id":N}: client ping
        return $self->_send_obj({ id => $id + 0 }) if defined $id && $self->{state} eq 'connected';
        return $self->disconnect(3501, 'bad request');
    }
    return $self->disconnect(3501, 'bad request') if $method ne 'send' && ref $cmd->{$method} ne 'HASH';

    my $state = $self->{state};
    if ($state eq 'new') {
        return $self->disconnect(3501, 'bad request') unless $method eq 'connect';
    }
    elsif ($state eq 'connecting') {
        return $self->disconnect(3501, 'bad request') if $method eq 'connect';
        push @{ $self->{queue} }, $cmd;
        return;
    }
    elsif ($method eq 'connect') {
        return $self->disconnect(3501, 'bad request');
    }

    my $h = "_h_$method";
    my $ok = eval { $self->$h($id, $cmd->{$method}); 1 };
    unless ($ok) {
        $self->{hub}->log_error("client $self->{id}: $method failed: $@");
        $self->_err($id, 100, 'internal server error', 1);
    }
}

# ---- hooks (proxy) --------------------------------------------------------
sub _outcome {
    my $r = shift;
    return ('error', { code => 100, message => 'internal server error', temporary => 1 }) unless ref $r eq 'HASH';
    return ('disconnect', $r->{disconnect}) if ref $r->{disconnect} eq 'HASH';
    return ('error', $r->{error}) if ref $r->{error} eq 'HASH';
    return ('ok', ref $r->{result} eq 'HASH' ? $r->{result} : {});
}

sub _call_hook {
    my ($self, $hook, $req, $cb) = @_;
    my $hub = $self->{hub};
    my $called = 0;
    my $timer;
    my $done = sub {
        return if $called++;
        $hub->scheduler->remove($timer);
        return if $self->{closed};
        $cb->(@_);
    };
    my $w = $done;
    $timer = $hub->scheduler->timer($hub->hook_timeout, sub {
        $w->({ error => { code => 100, message => 'internal server error', temporary => 1 } });
    });
    $req->{client}    = $self->{id};
    $req->{transport} = $self->{transport};
    $req->{protocol}  = 'json';
    $req->{encoding}  = 'json';
    my $ok = eval { $hook->($req, $done); 1 };
    unless ($ok) {
        $hub->log_error("hook failed: $@");
        $done->({ error => { code => 100, message => 'internal server error', temporary => 1 } });
    }
}

# Handles the non-ok outcomes of a hook. Returns true if it handled them.
sub _hook_failed {
    my ($self, $id, $kind, $val) = @_;
    return 0 if $kind eq 'ok';
    if ($kind eq 'disconnect') {
        $self->disconnect($val->{code} // 3503, $val->{reason} // '');
    }
    else {
        $self->_err($id, $val->{code} // 100, $val->{message} // 'error', $val->{temporary});
    }
    return 1;
}

# ---- connect --------------------------------------------------------------
sub _h_connect {
    my ($self, $id, $p) = @_;
    my $hub = $self->{hub};
    $self->{state} = 'connecting';

    my $fail = sub {
        my ($code, $msg, $close_code, $reason) = @_;
        $self->_err($id, $code, $msg) if defined $code;
        $self->disconnect($close_code, $reason);
    };

    if (defined $p->{token} && length $p->{token}) {
        return $fail->(undef, undef, 3500, 'invalid token') unless $hub->jwt;
        my ($claims, $why) = $hub->jwt->decode($p->{token});
        return $fail->(109, 'token expired', 3003, 'expired') if !$claims && $why eq 'expired';
        return $fail->(undef, undef, 3500, 'invalid token') unless $claims;
        return $self->_complete_connect($id, $p, {
            user      => $claims->{sub},
            expire_at => $claims->{exp},
            info      => $claims->{info},
            channels  => $claims->{channels},
        });
    }

    if (my $hook = $hub->hook('connect')) {
        return $self->_call_hook($hook, { name => $p->{name}, version => $p->{version}, data => $p->{data} }, sub {
            my ($kind, $val) = _outcome($_[0]);
            if ($kind eq 'disconnect') { return $self->disconnect($val->{code} // 3503, $val->{reason} // '') }
            if ($kind eq 'error') {
                $self->_err($id, $val->{code} // 100, $val->{message} // 'error', $val->{temporary});
                return $self->disconnect($val->{temporary} ? 3000 : 3503, 'connect rejected');
            }
            $self->_complete_connect($id, $p, $val);
        });
    }

    return $self->_complete_connect($id, $p, { user => '' }) if $hub->allow_anonymous;
    return $fail->(undef, undef, 3500, 'unauthorized');
}

sub _complete_connect {
    my ($self, $id, $p, $r) = @_;
    my $hub = $self->{hub};

    my $user = $r->{user};
    $user = '' unless defined $user;
    return $self->disconnect(3500, 'invalid token') if ref $user;
    my $exp = $r->{expire_at};
    if (defined $exp) {
        return $self->disconnect(3500, 'invalid token') if ref $exp || $exp !~ /\A\d+(?:\.\d+)?\z/;
        $exp = 0 + $exp;
        $exp = undef if $exp == 0;    # 0 means "no expiry" (Centrifugo convention)
    }

    $self->{user}       = "$user";
    $self->{conn_info}  = ref $r->{info} eq 'HASH' ? $r->{info} : undef;
    $self->{expire_at}  = $exp;
    $self->{state}      = 'connected';
    $self->{registered} = 1;
    $hub->scheduler->remove(delete $self->{timers}{connect});
    $hub->register($self);

    my $w = $self;
    weaken $w;
    $self->{timers}{ping} = $hub->scheduler->recurring($hub->ping_interval, sub { $w->_ping_tick if $w });
    $self->_arm_expire;

    # server-side subscriptions (JWT "channels" claim or connect proxy)
    my %subs;
    if (ref $r->{channels} eq 'ARRAY') {
        my $req = ref $p->{subs} eq 'HASH' ? $p->{subs} : {};
        for my $ch (@{ $r->{channels} }) {
            next if ref $ch;
            my ($opts) = $hub->namespaces->resolve($ch);
            next if !$opts || $self->{subs}{$ch};
            my $res = eval { $self->_do_subscribe($ch, $opts, ref $req->{$ch} eq 'HASH' ? $req->{$ch} : {}, {}) };
            if ($res) { $subs{$ch} = $res }
            else { $hub->log_warn("client $self->{id}: server-side subscribe to '$ch' failed" . ($@ ? ": $@" : '')) }
        }
    }

    my %res = (client => $self->{id}, version => $hub->version, ping => $hub->ping_interval + 0, pong => TRUE);
    if (defined $exp) { $res{expires} = TRUE; $res{ttl} = _ttl($exp, $hub->now) }
    $res{subs} = \%subs if %subs;
    $res{data} = $r->{data} if defined $r->{data};
    $self->_reply($id, 'connect', \%res);

    my @q = splice @{ $self->{queue} };
    for my $c (@q) {
        last if $self->{closed};
        $self->_dispatch($c);
    }
}

sub _ttl { my ($exp, $now) = @_; my $t = int($exp - $now); return $t < 0 ? 0 : $t }

# ---- keepalive & expiry ---------------------------------------------------
sub _ping_tick {
    my $self = shift;
    return if $self->{closed} || $self->{state} ne 'connected';
    return if $self->{awaiting_pong};    # previous ping still pending, pong timer will handle it
    my $hub = $self->{hub};
    $self->{awaiting_pong} = 1;
    $self->_send('{}');
    my $w = $self;
    weaken $w;
    $self->{timers}{pong} = $hub->scheduler->timer($hub->pong_timeout, sub {
        $w->disconnect(3009, 'no pong') if $w && $w->{awaiting_pong};
    });
}

sub _arm_expire {
    my $self = shift;
    my $hub = $self->{hub};
    $hub->scheduler->remove(delete $self->{timers}{expire});
    return unless defined $self->{expire_at};
    my $delay = $self->{expire_at} - $hub->now + $hub->expired_close_delay;
    $delay = 0 if $delay < 0;
    my $w = $self;
    weaken $w;
    $self->{timers}{expire} = $hub->scheduler->timer($delay, sub { $w->disconnect(3003, 'expired') if $w });
}

# ---- subscriptions --------------------------------------------------------
sub client_info {
    my ($self, $ch) = @_;
    my %i = (client => $self->{id}, user => $self->{user});
    $i{conn_info} = $self->{conn_info} if $self->{conn_info};
    my $s = defined $ch ? $self->{subs}{$ch} : undef;
    $i{chan_info} = $s->{chan_info} if $s && $s->{chan_info};
    return \%i;
}

sub _h_subscribe {
    my ($self, $id, $p) = @_;
    my $hub = $self->{hub};
    my $ch  = $p->{channel};

    my ($opts, $why) = $hub->namespaces->resolve($ch);
    return $self->_err($id, ($why // '') eq 'unknown' ? 102 : 107, ($why // '') eq 'unknown' ? 'unknown channel' : 'bad request')
        unless $opts;
    return $self->_err($id, 105, 'already subscribed') if $self->{subs}{$ch} || $self->{pending}{$ch};
    return $self->_err($id, 106, 'limit exceeded')
        if keys(%{ $self->{subs} }) + keys(%{ $self->{pending} }) >= $hub->client_channel_limit;

    my %ctx;
    if (defined $p->{token} && length $p->{token}) {
        my ($claims, $reason) = $hub->jwt ? $hub->jwt->decode($p->{token}) : (undef, 'no_jwt');
        return $self->_err($id, 109, 'token expired') if !$claims && ($reason // '') eq 'expired';
        return $self->_err($id, 103, 'permission denied')
            unless $claims && ($claims->{sub} // '') eq $self->{user} && ($claims->{channel} // '') eq $ch;
        $ctx{exp}       = $claims->{exp};
        $ctx{chan_info} = $claims->{info} if ref $claims->{info} eq 'HASH';
        return $self->_finish_subscribe($id, $ch, $opts, $p, \%ctx);
    }

    if ($opts->{allow_user_limited_channels}) {
        my $ul = $hub->namespaces->user_limited($ch, $self->{user});
        if (defined $ul) {
            return $ul ? $self->_finish_subscribe($id, $ch, $opts, $p, \%ctx) : $self->_err($id, 103, 'permission denied');
        }
    }
    return $self->_finish_subscribe($id, $ch, $opts, $p, \%ctx) if $opts->{allow_subscribe_for_client};

    if ($opts->{subscribe_proxy} && (my $hook = $hub->hook('subscribe'))) {
        $self->{pending}{$ch} = 1;
        return $self->_call_hook($hook, { user => $self->{user}, channel => $ch, data => $p->{data} }, sub {
            delete $self->{pending}{$ch};
            my ($kind, $val) = _outcome($_[0]);
            return if $self->_hook_failed($id, $kind, $val);
            $ctx{chan_info} = $val->{info} if ref $val->{info} eq 'HASH';
            $self->_finish_subscribe($id, $ch, $opts, $p, \%ctx);
        });
    }
    return $self->_err($id, 103, 'permission denied');
}

sub _finish_subscribe {
    my ($self, $id, $ch, $opts, $p, $ctx) = @_;
    my $res = eval { $self->_do_subscribe($ch, $opts, $p, $ctx) };
    unless ($res) {
        $self->{hub}->log_error("client $self->{id}: subscribe '$ch' failed: $@");
        return $self->_err($id, 100, 'internal server error', 1);
    }
    $self->_reply($id, 'subscribe', $res);
}

# Registers the subscription and returns the "subscribe" result. No permission
# logic here; callers decide. Rolls back on failure.
sub _do_subscribe {
    my ($self, $ch, $opts, $p, $ctx) = @_;
    my $hub = $self->{hub};
    my $sub = {
        opts       => $opts,
        chan_info  => $ctx->{chan_info},
        exp        => $ctx->{exp},
        positioned => $opts->{positioned},
    };
    $self->{subs}{$ch} = $sub;
    $hub->add_sub($self, $ch);

    my %res;
    my $ok = eval {
        my $info = $self->client_info($ch);
        $hub->presence_add($ch, $info) if $opts->{presence};
        $hub->emit_presence('join', $ch, $info) if $opts->{join_leave};

        if ($opts->{positioned}) {
            my ($offset, $epoch);
            if ($opts->{recoverable} && $p->{recover}) {
                my $so = $p->{offset};
                $so = 0 unless defined $so && !ref $so && $so =~ /\A\d+\z/;
                my $se = (defined $p->{epoch} && !ref $p->{epoch}) ? "$p->{epoch}" : '';
                my ($recovered, $pubs);
                ($recovered, $pubs, $offset, $epoch) = $hub->recover($ch, $opts, $so + 0, $se);
                $res{was_recovering} = TRUE;
                if ($recovered) {
                    $res{recovered} = TRUE;
                    $res{publications} = [ map { { data => $_->{data}, offset => $_->{offset} + 0, (ref $_->{info} eq 'HASH' ? (info => $_->{info}) : ()) } } @$pubs ]
                        if @$pubs;
                }
            }
            else {
                ($offset, $epoch) = $hub->stream_position($ch, $opts);
            }
            $res{recoverable} = TRUE if $opts->{recoverable};
            $res{positioned}  = TRUE;
            $res{epoch}       = str($epoch);
            $res{offset}      = $offset + 0;
            $sub->{offset}    = $offset + 0;
            $sub->{epoch}     = str($epoch);
        }
        1;
    };
    unless ($ok) {
        my $e = $@;
        $self->_drop_sub($ch, 1);
        die $e;
    }

    if (defined $sub->{exp} && $sub->{exp} != 0) {
        $res{expires} = TRUE;
        $res{ttl}     = _ttl($sub->{exp}, $hub->now);
        $self->_arm_sub_expire($ch);
    }
    return \%res;
}

sub _arm_sub_expire {
    my ($self, $ch) = @_;
    my $hub = $self->{hub};
    my $sub = $self->{subs}{$ch} or return;
    $hub->scheduler->remove(delete $sub->{timer});
    return unless defined $sub->{exp} && $sub->{exp} != 0;
    my $delay = $sub->{exp} - $hub->now + $hub->expired_close_delay;
    $delay = 0 if $delay < 0;
    my $w = $self;
    weaken $w;
    $sub->{timer} = $hub->scheduler->timer($delay, sub { $w->disconnect(3004, 'subscription expired') if $w });
}

sub _drop_sub {
    my ($self, $ch, $quiet) = @_;
    my $hub = $self->{hub};
    my $sub = delete $self->{subs}{$ch} or return;
    $hub->scheduler->remove(delete $sub->{timer});
    my $info = $self->client_info;
    $info->{chan_info} = $sub->{chan_info} if $sub->{chan_info};
    $hub->remove_sub($self, $ch);
    my $err;
    if ($sub->{opts}{presence}) { eval { $hub->presence_remove($ch, $self->{id}); 1 } or $err = $@ }
    if ($sub->{opts}{join_leave} && !$quiet) { eval { $hub->emit_presence('leave', $ch, $info); 1 } or $err //= $@ }
    die $err if $err && !$quiet;
}

sub _h_unsubscribe {
    my ($self, $id, $p) = @_;
    my $ch = $p->{channel};
    return $self->_err($id, 107, 'bad request') unless defined $ch && !ref $ch && length $ch;
    $self->_drop_sub($ch) if $self->{subs}{$ch};
    $self->_reply($id, 'unsubscribe', {});
}

# Called by the hub (server API unsubscribe).
sub server_unsubscribe {
    my ($self, $ch) = @_;
    return if $self->{closed} || !defined $ch || !$self->{subs}{$ch};
    eval { $self->_drop_sub($ch); 1 } or $self->{hub}->log_warn("server unsubscribe failed: $@");
    $self->_send_obj({ push => { channel => $ch, unsubscribe => { code => 2000, reason => 'server unsubscribe' } } });
}

sub refresh_presence {
    my $self = shift;
    return if $self->{closed};
    my $hub = $self->{hub};
    for my $ch (keys %{ $self->{subs} }) {
        $hub->presence_add($ch, $self->client_info($ch)) if $self->{subs}{$ch}{opts}{presence};
    }
}

# ---- pushes from the hub --------------------------------------------------
sub deliver_pub {
    my ($self, $ch, $offset, $epoch, $frame) = @_;
    return if $self->{closed};
    my $s = $self->{subs}{$ch} or return;
    if ($s->{positioned} && $offset > 0) {
        if (length $epoch && defined $s->{epoch} && length $s->{epoch} && $epoch ne $s->{epoch}) {
            return $self->disconnect(3007, 'insufficient state');
        }
        return if $offset <= $s->{offset};                        # duplicate
        return $self->disconnect(3007, 'insufficient state')     # gap -> client reconnects and recovers
            if $offset != $s->{offset} + 1;
        $s->{offset} = $offset;
    }
    $self->_send($frame);
}

sub deliver_event {
    my ($self, $ch, $frame) = @_;
    return if $self->{closed} || !$self->{subs}{$ch};
    $self->_send($frame);
}

# ---- publish / presence / history -----------------------------------------
sub _channel_opts {
    my ($self, $id, $p) = @_;
    my ($opts, $why) = $self->{hub}->namespaces->resolve($p->{channel});
    unless ($opts) {
        $self->_err($id, ($why // '') eq 'unknown' ? 102 : 107, ($why // '') eq 'unknown' ? 'unknown channel' : 'bad request');
        return;
    }
    return $opts;
}

sub _h_publish {
    my ($self, $id, $p) = @_;
    my $hub  = $self->{hub};
    my $opts = $self->_channel_opts($id, $p) or return;
    my $ch   = $p->{channel};
    return $self->_err($id, 107, 'bad request') unless exists $p->{data};

    my $do = sub {
        my ($data, $skip_history) = @_;
        my ($r, $err) = eval { $hub->publish($ch, $data, info => $self->client_info($ch), skip_history => $skip_history) };
        if ($@) {
            $hub->log_error("client $self->{id}: publish failed: $@");
            return $self->_err($id, 100, 'internal server error', 1);
        }
        return $self->_err($id, 107, 'bad request') unless $r;
        $self->_reply($id, 'publish', {});
    };

    if ($opts->{publish_proxy} && (my $hook = $hub->hook('publish'))) {
        return $self->_call_hook($hook, { user => $self->{user}, channel => $ch, data => $p->{data} }, sub {
            my ($kind, $val) = _outcome($_[0]);
            return if $self->_hook_failed($id, $kind, $val);
            $do->(exists $val->{data} ? $val->{data} : $p->{data}, $val->{skip_history});
        });
    }
    my $subscribed = exists $self->{subs}{$ch};
    return $self->_err($id, 103, 'permission denied')
        unless $opts->{allow_publish_for_client} || ($opts->{allow_publish_for_subscriber} && $subscribed);
    $do->($p->{data}, 0);
}

sub _h_presence {
    my ($self, $id, $p) = @_;
    my $opts = $self->_channel_opts($id, $p) or return;
    return $self->_err($id, 108, 'not available') unless $opts->{presence};
    return $self->_err($id, 103, 'permission denied')
        unless $opts->{allow_presence_for_client} || ($opts->{allow_presence_for_subscriber} && $self->{subs}{ $p->{channel} });
    my ($pres, $err) = $self->{hub}->presence($p->{channel});
    return $self->_err($id, 100, 'internal server error', 1) unless $pres;
    $self->_reply($id, 'presence', { presence => $pres });
}

sub _h_presence_stats {
    my ($self, $id, $p) = @_;
    my $opts = $self->_channel_opts($id, $p) or return;
    return $self->_err($id, 108, 'not available') unless $opts->{presence};
    return $self->_err($id, 103, 'permission denied')
        unless $opts->{allow_presence_for_client} || ($opts->{allow_presence_for_subscriber} && $self->{subs}{ $p->{channel} });
    my ($r, $err) = $self->{hub}->presence_stats($p->{channel});
    return $self->_err($id, 100, 'internal server error', 1) unless $r;
    $self->_reply($id, 'presence_stats', $r);
}

sub _h_history {
    my ($self, $id, $p) = @_;
    my $opts = $self->_channel_opts($id, $p) or return;
    return $self->_err($id, 108, 'not available') unless $opts->{history};
    return $self->_err($id, 103, 'permission denied')
        unless $opts->{allow_history_for_client} || ($opts->{allow_history_for_subscriber} && $self->{subs}{ $p->{channel} });
    my ($r, $err) = $self->{hub}->history($p->{channel}, limit => $p->{limit}, since => $p->{since}, reverse => $p->{reverse});
    unless ($r) {
        return $self->_err($id, 112, 'unrecoverable position') if ($err // '') eq 'unrecoverable';
        return $self->_err($id, 107, 'bad request');
    }
    $self->_reply($id, 'history', $r);
}

# ---- rpc / refresh / misc -------------------------------------------------
sub _h_rpc {
    my ($self, $id, $p) = @_;
    my $hook = $self->{hub}->hook('rpc') or return $self->_err($id, 104, 'method not found');
    $self->_call_hook($hook, { user => $self->{user}, method => $p->{method}, data => $p->{data} }, sub {
        my ($kind, $val) = _outcome($_[0]);
        return if $self->_hook_failed($id, $kind, $val);
        $self->_reply($id, 'rpc', exists $val->{data} ? { data => $val->{data} } : {});
    });
}

sub _h_refresh {
    my ($self, $id, $p) = @_;
    my $hub = $self->{hub};

    my $apply = sub {
        my $exp = shift;
        if (defined $exp && !ref $exp && $exp =~ /\A\d+(?:\.\d+)?\z/ && $exp != 0) { $self->{expire_at} = 0 + $exp }
        else { $self->{expire_at} = undef }
        $self->_arm_expire;
        my %res = (client => $self->{id}, version => $hub->version);
        if (defined $self->{expire_at}) { $res{expires} = TRUE; $res{ttl} = _ttl($self->{expire_at}, $hub->now) }
        $self->_reply($id, 'refresh', \%res);
    };

    if (defined $p->{token} && length $p->{token}) {
        my ($claims, $why) = $hub->jwt ? $hub->jwt->decode($p->{token}) : (undef, 'no_jwt');
        return $self->disconnect(3500, 'invalid token') unless $claims && ($claims->{sub} // '') eq $self->{user};
        $self->{conn_info} = $claims->{info} if ref $claims->{info} eq 'HASH';
        return $apply->($claims->{exp});
    }
    if (my $hook = $hub->hook('refresh')) {
        return $self->_call_hook($hook, { user => $self->{user} }, sub {
            my ($kind, $val) = _outcome($_[0]);
            return if $self->_hook_failed($id, $kind, $val);
            return $self->disconnect(3003, 'expired') if $val->{expired};
            $self->{conn_info} = $val->{info} if ref $val->{info} eq 'HASH';
            $apply->($val->{expire_at});
        });
    }
    $self->disconnect(3500, 'invalid token');
}

sub _h_sub_refresh {
    my ($self, $id, $p) = @_;
    my $hub = $self->{hub};
    my $ch  = $p->{channel};
    my $sub = (defined $ch && !ref $ch) ? $self->{subs}{$ch} : undef;
    return $self->_err($id, 107, 'bad request') unless $sub && defined $p->{token};
    my ($claims, $why) = $hub->jwt ? $hub->jwt->decode($p->{token}) : (undef, 'no_jwt');
    return $self->disconnect(3004, 'subscription expired') unless $claims
        && ($claims->{sub} // '') eq $self->{user} && ($claims->{channel} // '') eq $ch;
    $sub->{exp} = $claims->{exp};
    $self->_arm_sub_expire($ch);
    my %res;
    if (defined $sub->{exp} && $sub->{exp} != 0) { $res{expires} = TRUE; $res{ttl} = _ttl($sub->{exp}, $hub->now) }
    $self->_reply($id, 'sub_refresh', \%res);
}

sub _h_send { }    # async client->server messages are accepted and ignored

1;
