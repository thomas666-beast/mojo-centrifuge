package Mojo::Centrifuge;
use Mojo::Base -base;
use Mojo::JSON qw(encode_json decode_json);
use Mojo::IOLoop;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::Protocol;
use Mojo::Centrifuge::Protocol qw(:codes);

our $VERSION = '0.21';

has 'log';
has 'redis';
has 'history_size'   => 100;
has 'history_ttl'    => 0;
has 'ping_interval'  => 25;
has 'pong_timeout'   => 10;
has 'presence_ttl'   => 60;
has 'protocol'       => 'legacy';

has 'channels'    => sub { {} };
has 'connections' => sub { {} };

has '_counter' => 0;
has 'prefix'   => 'centrifuge';
has 'presence_events' => 1;
has 'auth';
has 'proto'    => sub { Mojo::Centrifuge::Protocol->new };
has '_node_id' => sub { sprintf "%d-%d", $$, int(rand(1_000_000)) };

sub _valid_channel {
    my $ch = shift;
    return defined $ch && !ref $ch && length $ch && length $ch <= 255 ? 1 : 0;
}

sub _sid    { my ($self, $id) = @_; return $self->_node_id . "-$id" }
sub _member { my ($self, $id, $user) = @_; return $self->_sid($id) . ":$user" }

sub _history_key  { my ($self, $ch) = @_; return $self->prefix . ":history:$ch" }
sub _seq_key      { my ($self, $ch) = @_; return $self->prefix . ":seq:$ch" }
sub _pubsub_ch    { my ($self, $ch) = @_; return $self->prefix . ":pubsub:$ch" }
sub _presence_key { my ($self, $ch) = @_; return $self->prefix . ":presence:$ch" }

sub _wire_pub {
    my ($self, $channel, $seq, $data) = @_;
    return $self->protocol eq 'legacy'
        ? encode_json({ channel => $channel, seq => $seq, data => $data })
        : $self->proto->push_frame(channel => $channel, seq => $seq, data => $data);
}

sub _wire_join_leave {
    my ($self, $channel, $event, $id, $user) = @_;
    return $self->protocol eq 'legacy'
        ? encode_json({
            channel => $channel,
            event   => $event,
            id      => $self->_sid($id),
            user    => $user,
        })
        : $self->proto->push_frame(
            channel => $channel,
            $event  => { client => $self->_sid($id), user => $user },
        );
}

sub _presence_event {
    my ($self, $channel, $event, $id, $user) = @_;
    return unless $self->presence_events;

    $self->_local_send($channel, $self->_wire_join_leave($channel, $event, $id, $user), $id);
    $self->redis->pubsub->notify(
        $self->_pubsub_ch($channel),
        encode_json({
            channel => $channel,
            event   => $event,
            id      => $self->_sid($id),
            user    => $user,
            type    => 'presence',
            node    => $self->_node_id,
        }),
    );
}

sub authenticate {
    my ($self, $token) = @_;

    return { user => 'anonymous', bound_channel => undef, raw => {}, token => undef }
        unless $self->auth;

    my $payload = $self->auth->verify_token($token) or return undef;

    return {
        user          => $payload->{user}     // 'anonymous',
        bound_channel => $payload->{channel}  // undef,
        channels      => $payload->{channels} // undef,
        raw           => $payload,
        token         => $token,
    };
}

sub _match {
    my ($self, $pattern, $channel) = @_;
    return 1 if !defined $pattern;
    my $re = quotemeta $pattern;
    $re =~ s/\\\*/.*/g;
    $re =~ s/\\\?/./g;
    return $channel =~ /^$re$/ ? 1 : 0;
}

sub _allowed {
    my ($self, $identity, $channel, $kind) = @_;
    return 0 unless $identity && ref $identity eq 'HASH';

    my $raw = $identity->{raw} // {};
    my $patterns =
          $raw->{$kind}     ? $raw->{$kind}
        : $raw->{channels}  ? $raw->{channels}
        : $raw->{channel}   ? [ $raw->{channel} ]
        :                     undef;

    return 0 unless $patterns;
    for my $p (@$patterns) {
        return 1 if $self->_match($p, $channel);
    }
    return 0;
}

sub add_connection {
    my ($self, $c, $auto_channel, $since, $identity) = @_;

    if (!ref $identity || ref $identity ne 'HASH') {
        $identity = $self->authenticate($identity);
        unless ($identity) {
            $self->log->warn("Auth failed") if $self->log;
            $c->rendered(401);
            return;
        }
    }

    my $user = $identity->{user} // 'anonymous';

    my $id = $self->_counter + 1;
    $self->_counter($id);

    $c->inactivity_timeout(300);

    my $tx  = $c->tx;
    my $now = time;

    $self->connections->{$id} = {
        user          => $user,
        identity      => $identity,
        tx            => $tx,
        last_pong     => $now,
        awaiting_pong => 0,
    };

    $self->log->info("Session $id ($user) opened") if $self->log;

    $c->on(message => sub {
        my ($tx, $raw) = @_;
        if ($self->protocol eq 'centrifuge') {
            return $self->_on_message_centrifuge($id, $tx, $raw);
        }
        return $self->_on_message_legacy($id, $tx, $raw);
    });

    $tx->on(finish => sub {
        $self->drop_session($id);
        $self->log->info("Session $id ($user) closed") if $self->log;
    });

    if (defined $auto_channel && length $auto_channel) {
        unless ($self->_allowed($identity, $auto_channel, 'subscribe')) {
            $self->log->warn("Auto-subscribe denied: '$auto_channel'") if $self->log;
            $tx->send($self->_error_payload(
                $self->protocol eq 'legacy' ? ERR_PERMISSION_DENIED : ERR_PERMISSION_DENIED,
                "not authorized for channel '$auto_channel'",
            ));
        }
        else {
            $self->subscribe($id, $auto_channel, $since);
        }
    }

    $self->_schedule_ping($id);

    return $id;
}

sub _error_payload {
    my ($self, $code, $message) = @_;
    return $self->protocol eq 'legacy'
        ? encode_json({ error => { code => $code, message => $message } })
        : $self->proto->error_frame(undef, $code, $message);
}

sub _on_message_legacy {
    my ($self, $id, $tx, $raw) = @_;

    my $cmd = eval { decode_json($raw) };
    unless (ref $cmd eq 'HASH') {
        $self->log->warn("Session $id sent non-object message") if $self->log;
        return;
    }

    if ($cmd->{pong}) {
        my $sess = $self->connections->{$id};
        if ($sess) {
            $sess->{last_pong}     = time;
            $sess->{awaiting_pong} = 0;
        }
        return;
    }
    if ($cmd->{ping}) {
        $tx->send(encode_json({ pong => 1 }));
        return;
    }

    my $reply = sub {
        my $h = shift;
        $h->{id} = $cmd->{id} if defined $cmd->{id} && !ref $cmd->{id};
        $tx->send(encode_json($h));
    };
    my $reply_error = sub {
        my ($code, $msg) = @_;
        $reply->({ error => { code => $code, message => $msg } });
    };

    for my $k (qw(subscribe unsubscribe publish presence)) {
        next unless exists $cmd->{$k};
        next if _valid_channel($cmd->{$k});
        $reply_error->(ERR_BAD_REQUEST, "invalid channel in '$k'");
        return;
    }

    my $sess = $self->connections->{$id} or return;

    if (my $sub = $cmd->{subscribe}) {
        unless ($self->_allowed($sess->{identity}, $sub, 'subscribe')) {
            $self->log->warn("Session $id denied subscribe to '$sub'") if $self->log;
            $reply_error->(ERR_PERMISSION_DENIED, "not authorized for channel '$sub'");
            return;
        }
        $self->subscribe($id, $sub, $cmd->{since});
    }
    elsif (my $unsub = $cmd->{unsubscribe}) {
        $self->unsubscribe($id, $unsub);
    }
    elsif (my $pch = $cmd->{presence}) {
        unless ($self->_allowed($sess->{identity}, $pch, 'subscribe')) {
            $reply_error->(ERR_PERMISSION_DENIED, "not authorized for channel '$pch'");
            return;
        }
        my $list = eval { $self->presence($pch) } // [];
        $reply->({ channel => $pch, presence => $list });
    }
    elsif (my $pub = $cmd->{publish}) {
        unless (exists $cmd->{data}) {
            $reply_error->(ERR_BAD_PUBLISH, "publish requires 'data'");
            return;
        }
        unless ($self->can_publish($id, $pub)) {
            $self->log->warn("Session $id denied publish to '$pub'") if $self->log;
            $reply_error->(ERR_PERMISSION_DENIED, "not authorized to publish to '$pub'");
            return;
        }
        my $seq = eval { $self->publish($pub, $cmd->{data}) };
        if (defined $seq) {
            $reply->({ published => $seq, channel => $pub });
        }
        else {
            $self->log->error("Session $id publish failed: $@") if $self->log;
            $reply_error->(ERR_INTERNAL, "publish failed");
        }
    }
    else {
        $self->log->warn("Session $id sent unknown command") if $self->log;
    }
}

sub _on_message_centrifuge {
    my ($self, $id, $tx, $raw) = @_;

    my $f = $self->proto->decode_frame($raw);
    my $sess = $self->connections->{$id} or return;

    unless ($f) {
        $tx->send($self->proto->error_frame(undef, ERR_BAD_REQUEST, 'bad request'));
        return;
    }

    if ($f->{empty} || $f->{method} eq 'pong') {
        $sess->{last_pong}     = time;
        $sess->{awaiting_pong} = 0;
        return;
    }

    my $reply = sub {
        my ($payload) = @_;
        $tx->send($self->proto->result_frame($f->{id}, $payload));
    };
    my $error = sub {
        my ($code, $msg) = @_;
        $tx->send($self->proto->error_frame($f->{id}, $code, $msg));
    };

    my $m = $f->{method};
    my $p = $f->{params} // {};

    if ($m eq 'ping') {
        $tx->send($self->proto->pong_frame);
        return;
    }

    if ($m eq 'connect') {
        return $reply->({
            client  => $self->_sid($id),
            version => $VERSION,
            expires => 1,
            ping    => $self->ping_interval,
            pong    => $self->pong_timeout,
            server  => $self->_node_id,
        });
    }

    if ($m eq 'subscribe') {
        my $ch = $p->{channel};
        return $error->(ERR_BAD_SUBSCRIBE, 'bad channel') unless _valid_channel($ch);
        return $error->(ERR_PERMISSION_DENIED, "not authorized for channel '$ch'")
            unless $self->_allowed($sess->{identity}, $ch, 'subscribe');
        $self->subscribe($id, $ch, $p->{since});
        return $reply->({ channel => $ch });
    }

    if ($m eq 'unsubscribe') {
        my $ch = $p->{channel};
        return $error->(ERR_BAD_SUBSCRIBE, 'bad channel') unless _valid_channel($ch);
        $self->unsubscribe($id, $ch);
        return $reply->({});
    }

    if ($m eq 'presence') {
        my $ch = $p->{channel};
        return $error->(ERR_BAD_SUBSCRIBE, 'bad channel') unless _valid_channel($ch);
        return $error->(ERR_PERMISSION_DENIED, "not authorized for channel '$ch'")
            unless $self->_allowed($sess->{identity}, $ch, 'subscribe');
        my $list = eval { $self->presence($ch) } // [];
        return $reply->({ channel => $ch, presence => $list });
    }

    if ($m eq 'publish') {
        my $ch = $p->{channel};
        return $error->(ERR_BAD_PUBLISH, 'bad channel') unless _valid_channel($ch);
        return $error->(ERR_BAD_PUBLISH, "publish requires 'data'")
            unless exists $p->{data};
        return $error->(ERR_PERMISSION_DENIED, "not authorized to publish to '$ch'")
            unless $self->can_publish($id, $ch);
        my $seq = eval { $self->publish($ch, $p->{data}) };
        return defined $seq
            ? $reply->({ channel => $ch, offset => $seq })
            : $error->(ERR_INTERNAL, 'publish failed');
    }

    if ($m eq 'refresh') {
        my $tok = $p->{token};
        my $new = $self->auth ? $self->auth->verify_token($tok) : undef;
        return $error->(ERR_TOKEN_EXPIRED, 'token expired or invalid') unless $new;
        $sess->{identity}{raw}   = $new;
        $sess->{identity}{user}  = $new->{user} // 'anonymous';
        $sess->{identity}{token} = $tok;
        return $reply->({ client => $self->_sid($id), expires => 1 });
    }

    return $error->(ERR_UNKNOWN_METHOD, "unknown method '$m'");
}

sub authorize_publish {
    my ($self, $identity, $channel) = @_;
    return $self->_allowed($identity, $channel, 'publish');
}

sub can_publish {
    my ($self, $id, $channel) = @_;
    my $sess = $self->connections->{$id} or return 0;
    return $self->_allowed($sess->{identity}, $channel, 'publish');
}

sub _schedule_ping {
    my ($self, $id) = @_;
    my $timer = Mojo::IOLoop->recurring($self->ping_interval => sub {
        my $sess = $self->connections->{$id} or return;
        my $tx   = $sess->{tx};
        return unless $tx && !$tx->is_finished;

        if ($sess->{awaiting_pong}) {
            my $age = time - $sess->{last_pong};
            if ($age > $self->ping_interval + $self->pong_timeout) {
                $self->log->warn("Session $id: pong timeout (${age}s), closing") if $self->log;
                eval { $tx->finish };
                return;
            }
        }

        if ($self->protocol eq 'centrifuge') {
            eval { $tx->send($self->proto->ping_frame) };
        }
        else {
            eval { $tx->send(encode_json({ ping => 1 })) };
        }
        $sess->{awaiting_pong} = 1;
    });
    $self->connections->{$id}{ping_timer} = $timer if $self->connections->{$id};
}

sub subscribe {
    my ($self, $id, $channel, $since) = @_;
    return unless $channel && length $channel;

    my $sess = $self->connections->{$id} or return;
    my $tx   = $sess->{tx};
    return unless $tx && !$tx->is_finished;

    my $is_new = !$sess->{subscribed}{$channel};
    $sess->{subscribed}{$channel}++;
    $self->channels->{$channel}{$id} = $tx;
    $self->_ensure_subscribed($channel);

    $self->redis->db->zadd(
        $self->_presence_key($channel), time, $self->_member($id, $sess->{user}),
    );
    $self->_presence_event($channel, 'join', $id, $sess->{user}) if $is_new;

    $self->log->info("Session $id ($sess->{user}) subscribed to '$channel'") if $self->log;

    if (defined $since && $since =~ /^\d+$/) {
        my @missed = @{ $self->history_for($channel, $since) };
        for my $m (@missed) {
            $tx->send($self->_wire_pub($channel, $m->{seq}, $m->{data}));
        }
    }
}

sub unsubscribe {
    my ($self, $id, $channel) = @_;
    return unless $channel && length $channel;

    my $sess = $self->connections->{$id} or return;
    return unless $sess->{subscribed}{$channel};

    delete $sess->{subscribed}{$channel};
    delete $self->channels->{$channel}{$id};
    delete $self->channels->{$channel}
        unless keys %{ $self->channels->{$channel} // {} };
    $self->redis->db->zrem($self->_presence_key($channel), $self->_member($id, $sess->{user}));
    $self->_presence_event($channel, 'leave', $id, $sess->{user});

    $self->log->info("Session $id unsubscribed from '$channel'") if $self->log;
}

sub drop_session {
    my ($self, $id) = @_;
    my $sess = $self->connections->{$id} or return;
    delete $self->connections->{$id};
    Mojo::IOLoop->remove($sess->{ping_timer}) if $sess->{ping_timer};

    for my $ch (keys %{ $sess->{subscribed} // {} }) {
        delete $self->channels->{$ch}{$id};
        delete $self->channels->{$ch}
            unless keys %{ $self->channels->{$ch} // {} };
        $self->redis->db->zrem($self->_presence_key($ch), $self->_member($id, $sess->{user}));
        $self->_presence_event($ch, 'leave', $id, $sess->{user});
    }
}

sub publish {
    my ($self, $channel, $data) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $seq = $self->redis->db->incr($self->_seq_key($channel));

    my $redis_payload = encode_json({
        seq     => $seq,
        data    => $data,
        channel => $channel,
        node    => $self->_node_id,
    });

    my $hkey = $self->_history_key($channel);
    $self->redis->db->lpush($hkey, $redis_payload);
    $self->redis->db->ltrim($hkey, 0, $self->history_size - 1);
    $self->redis->db->expire($hkey, $self->history_ttl) if $self->history_ttl > 0;

    $self->_local_send($channel, $self->_wire_pub($channel, $seq, $data));
    $self->redis->pubsub->notify($self->_pubsub_ch($channel), $redis_payload);

    return $seq;
}

sub _ensure_subscribed {
    my ($self, $channel) = @_;
    return if $self->{_subscribed}{$channel}++;

    my $pubsub = $self->redis->pubsub;
    $pubsub->listen($self->_pubsub_ch($channel) => sub {
        my ($pubsub, $message, $ch) = @_;
        my $m = eval { decode_json($message) } or return;
        return if $m->{node} && $m->{node} eq $self->_node_id;

        if (($m->{type} // '') eq 'presence') {
            $self->_local_send(
                $channel,
                $self->_wire_join_leave($channel, $m->{event}, $m->{id}, $m->{user}),
            );
            return;
        }

        $self->_local_send(
            $channel,
            $self->_wire_pub($m->{channel} // $channel, $m->{seq}, $m->{data}),
        );
    });
}

sub _local_send {
    my ($self, $channel, $payload, $skip) = @_;
    my $subs = $self->channels->{$channel} // {};
    my $sent = 0;
    for my $id (keys %$subs) {
        next if defined $skip && $id eq $skip;
        my $tx = $subs->{$id};
        next unless $tx && !$tx->is_finished;
        my $ok = eval { $tx->send($payload); 1 };
        unless ($ok) { delete $subs->{$id}; next }
        $sent++;
    }
    delete $self->channels->{$channel}
        if exists $self->channels->{$channel} && !keys %$subs;
    return $sent;
}

sub broadcast {
    my ($self, $channel, $msg) = @_;
    return $self->publish($channel, $msg);
}

sub history_for {
    my ($self, $channel, $since) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $raw = $self->redis->db->lrange($self->_history_key($channel), 0, $self->history_size - 1) // [];
    my @all = map { decode_json($_) } @$raw;
    @all = reverse @all;

    return [ @all ] unless defined $since && $since =~ /^\d+$/;
    return [ grep { $_->{seq} > $since } @all ];
}

sub presence {
    my ($self, $channel) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $key = $self->_presence_key($channel);
    $self->redis->db->zremrangebyscore($key, '-inf', time - $self->presence_ttl)
        if $self->presence_ttl > 0;

    my $members = $self->redis->db->zrange($key, 0, -1) // [];
    my @users;
    for my $m (@$members) {
        my ($id, $user) = split /:/, $m, 2;
        push @users, { id => $id, user => $user // 'anonymous' };
    }
    return [ sort { $a->{user} cmp $b->{user} || $a->{id} cmp $b->{id} } @users ];
}

sub channel_count {
    my ($self, $channel) = @_;
    my $subs = $self->channels->{$channel} or return 0;
    return scalar keys %$subs;
}

sub total_count {
    my $self = shift;
    my $n = 0;
    $n += scalar keys %{ $self->channels->{$_} } for keys %{ $self->channels };
    return $n;
}

sub stats {
    my $self = shift;
    my %channels;
    for my $ch (sort keys %{ $self->channels }) {
        my @ids = sort { $a <=> $b } keys %{ $self->channels->{$ch} };
        my $h   = $self->history_for($ch);
        $channels{$ch} = {
            connections => scalar(@ids),
            ids         => \@ids,
            last_seq    => $h->[-1]{seq} // 0,
            history_len => scalar @$h,
        };
    }

    return {
        connections => $self->total_count,
        channels    => \%channels,
        sessions    => scalar keys %{ $self->connections },
        timestamp   => time,
        version     => $VERSION,
    };
}

1;

__END__

=head1 NAME

Mojo::Centrifuge - Centrifugo-like realtime pub/sub for Mojolicious

=head1 SYNOPSIS

  use Mojolicious::Lite;
  use Mojo::Redis;
  use Mojo::Centrifuge;
  use Mojo::Centrifuge::Auth;

  my $redis = Mojo::Redis->new('redis://127.0.0.1:6379/0');
  my $auth  = Mojo::Centrifuge::Auth->new(secret => $ENV{SECRET});
  my $hub   = Mojo::Centrifuge->new(
      log      => app->log,
      redis    => $redis,
      auth     => $auth,
      protocol => 'centrifuge',
  );

  websocket '/ws' => sub {
      my $c = shift;
      my $identity = $hub->authenticate($c->param('token'))
          or return $c->render(status => 401, text => "unauthorized\n");
      $hub->add_connection($c, $c->param('channel'), $c->param('since'), $identity);
  };

=head1 DESCRIPTION

A Redis-backed realtime pub/sub server. Supports subscribe/unsubscribe,
history with C<since> replay, presence, keepalive ping/pong, client
publish with per-channel authorization, and multi-node fan-out via
Redis pub/sub. Two wire formats are available: C<'legacy'> (the
envelope used before 0.21) and C<'centrifuge'> (Centrifugo JSON
protocol, via L<Mojo::Centrifuge::Protocol>).

=head1 ATTRIBUTES

L<Mojo::Centrifuge> inherits all attributes from L<Mojo::Base> and
implements the following.

=head2 log

L<Mojo::Log>-compatible logger. Optional.

=head2 redis

A L<Mojo::Redis> instance. Required.

=head2 auth

A L<Mojo::Centrifuge::Auth> instance. Optional; when absent, every
caller is authenticated as C<anonymous> with no channel restrictions.

=head2 history_size

Maximum number of publications retained per channel. Default 100.

=head2 history_ttl

Seconds after which the history key of a channel expires. C<0> (the
default) disables expiry. When positive, the TTL is refreshed on every
publish.

=head2 ping_interval

Seconds between server pings. Default 25.

=head2 pong_timeout

Seconds the server waits for a pong after a ping before closing the
connection. Default 10.

=head2 presence_ttl

Seconds after which a presence entry is considered stale and dropped.
Default 60. Stale entries are removed lazily on C<presence> queries.

=head2 protocol

C<'legacy'> (default) or C<'centrifuge'>. In C<'centrifuge'> mode the
WebSocket speaks the Centrifugo JSON protocol; in C<'legacy'> mode it
speaks the pre-0.21 envelope.

=head2 prefix

Redis key prefix. Default C<'centrifuge'>.

=head2 presence_events

When true (default), join/leave events are pushed to other subscribers
of the same channel.

=head1 METHODS

L<Mojo::Centrifuge> inherits all methods from L<Mojo::Base> and
implements the following.

=head2 authenticate

  my $identity = $hub->authenticate($token);

Verifies a client token and returns an identity hashref, or C<undef>
on failure. Call this B<before> the WebSocket upgrade and render a
real C<401> on failure.

=head2 add_connection

  my $id = $hub->add_connection($c, $channel, $since, $identity);

Registers a WebSocket connection. C<$c> must be a
L<Mojolicious::Controller> in a websocket action.

=head2 publish

  my $seq = $hub->publish($channel, $data);

Publishes C<$data> to C<$channel>. Returns the sequence number.

=head2 subscribe

  $hub->subscribe($id, $channel, $since);

Subscribes the given session to C<$channel>, optionally replaying
history after C<$since>.

=head2 unsubscribe

  $hub->unsubscribe($id, $channel);

Removes the session from C<$channel>.

=head2 history_for

  my $arrayref = $hub->history_for($channel, $since);

Returns retained publications of C<$channel> as an arrayref of
C<{seq, data}> hashrefs, newest last.

=head2 presence

  my $arrayref = $hub->presence($channel);

Returns current members of C<$channel> as an arrayref of
C<{id, user}>. Entries older than C<presence_ttl> are dropped first.

=head2 authorize_publish

  my $ok = $hub->authorize_publish($identity, $channel);

Returns true if C<$identity> may publish to C<$channel>.

=head2 can_publish

  my $ok = $hub->can_publish($session_id, $channel);

Session-scoped variant of C<authorize_publish>.

=head2 stats

  my $hashref = $hub->stats;

Returns counters: C<connections>, C<sessions>, C<channels>,
C<timestamp>, C<version>.

=head2 broadcast

  my $seq = $hub->broadcast($channel, $msg);

Alias for C<publish>. Returns the sequence number.

=head2 channel_count

  my $n = $hub->channel_count($channel);

Returns the number of local sessions subscribed to C<$channel> on this
node.

=head2 total_count

  my $n = $hub->total_count;

Returns the total number of local subscriptions across all channels on
this node.

=head2 drop_session

  $hub->drop_session($id);

Removes a session: unsubscribes it from all channels, updates presence,
stops its ping timer, and forgets it. Called automatically when the
WebSocket finishes; public so tests and admin endpoints can
force-disconnect.

=head1 AUTHORIZATION

Token permissions are evaluated with glob matching (C<*> and C<?>).
Fallback chain per operation:

  <operation> (subscribe|publish) -> channels -> channel -> deny

=head1 LICENSE

Same as Perl itself (see the LICENSE file).

=cut
