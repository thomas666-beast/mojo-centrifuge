package Mojo::Centrifuge;
use Mojo::Base -base;
use Mojo::JSON qw(encode_json decode_json);
use Mojo::IOLoop;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::Protocol;
use Mojo::Centrifuge::Protocol qw(:codes);

our $VERSION = '0.30';

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

has 'epoch_key_ttl' => 0;   # if >0, epoch key expires (matches history TTL semantics); 0 = never
has 'client_recover' => 1;  # global kill switch

has 'publish_rate_limit' => 0;
has 'message_rate_limit' => 0;

has 'client_channel_limit' => 128;
has 'namespaces' => sub { {} };
has 'epoch_key_ttl' => 0;

sub _namespace_of {
    my ($self, $channel) = @_;
    return '' unless defined $channel && length $channel;
    my $i = index($channel, ':');
    return '' if $i < 0;
    return substr($channel, 0, $i);
}

sub _channel_opts {
    my ($self, $channel) = @_;
    my $ns = $self->_namespace_of($channel);
    my $ns_cfg = $self->namespaces->{$ns} // {};

    return {
        history_size         => $ns_cfg->{history_size}         // $self->history_size,
        history_ttl          => $ns_cfg->{history_ttl}          // $self->history_ttl,
        presence_ttl         => $ns_cfg->{presence_ttl}         // $self->presence_ttl,
        presence             => $ns_cfg->{presence}             // 1,
        presence_events      => $ns_cfg->{presence_events}      // $self->presence_events,
        client_channel_limit => $ns_cfg->{client_channel_limit} // $self->client_channel_limit,
        force_recover        => $ns_cfg->{force_recover}        // 0,
        epoch_key_ttl        => $ns_cfg->{epoch_key_ttl}        // $self->epoch_key_ttl,
        force_recover        => $ns_cfg->{force_recover}        // 0,
    };
}

sub _epoch_key { my ($self, $ch) = @_; return $self->prefix . ":epoch:$ch" }

sub _new_epoch {
    my ($self, $ch) = @_;
    my $e = sprintf "%d-%d-%d", time, $$, int(rand(1_000_000_000));
    my $key = $self->_epoch_key($ch);
    $self->redis->db->set($key, $e);
    my $opts = $self->_channel_opts($ch);
    $self->redis->db->expire($key, $opts->{epoch_key_ttl})
        if $opts->{epoch_key_ttl} > 0;
    return $e;
}

sub _current_epoch {
    my ($self, $ch) = @_;
    my $e = $self->redis->db->get($self->_epoch_key($ch));
    return $e if defined $e && length $e;
    return $self->_new_epoch($ch);
}

sub _reset_epoch { my ($self, $ch) = @_; return $self->_new_epoch($ch) }

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
    my ($self, $channel, $seq, $data, $tags) = @_;
    return $self->protocol eq 'legacy'
        ? encode_json({
            channel => $channel,
            seq     => $seq,
            data    => $data,
            (ref $tags eq 'HASH' && keys %$tags ? (tags => $tags) : ()),
          })
        : $self->proto->push_frame(
            channel => $channel,
            seq     => $seq,
            data    => $data,
            tags    => $tags,
          );
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

    my $opts = $self->_channel_opts($channel);
    return unless $opts->{presence_events};

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

    # reserved prefixes
    if ($self->_is_server_channel($channel)) {
        return 0 if $kind eq 'subscribe' || $kind eq 'publish';
        return 0;
    }
    if ($self->_is_user_channel($channel)) {
        my $u = $self->_user_channel_of($channel);
        my $me = $identity->{user} // '';
        return 0 unless defined $u && $u ne '' && $u eq $me;
    }

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

sub _is_server_channel { my ($self, $ch) = @_; return defined $ch && $ch =~ /^#/ }
sub _is_user_channel   { my ($self, $ch) = @_; return defined $ch && $ch =~ /^\$/ }
sub _user_channel_of   { my ($self, $ch) = @_; return $ch =~ /^\$([^:]+)/ ? $1 : undef }

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
    $self->_schedule_expiry($id);

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

    my $sess = $self->connections->{$id} or return;

    unless ($self->_rate_ok($sess, 'message', $self->message_rate_limit)) {
        $self->log->warn("Session $id: message rate limit exceeded, closing") if $self->log;
        eval { $tx->finish };
        $self->drop_session($id);
        return;
    }

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

    if (my $sub = $cmd->{subscribe}) {
        unless ($self->_allowed($sess->{identity}, $sub, 'subscribe')) {
            $self->log->warn("Session $id denied subscribe to '$sub'") if $self->log;
            $reply_error->(ERR_PERMISSION_DENIED, "not authorized for channel '$sub'");
            return;
        }

    my $existing = $sess->{subscribed}{$sub} ? 1 : 0;
    my $count    = scalar keys %{ $sess->{subscribed} // {} };
    my $limit    = $self->_channel_opts($sub)->{client_channel_limit};
    if (!$existing && $count >= $limit) {
        return $reply_error->(ERR_BAD_SUBSCRIBE, "channel limit exceeded ($limit)");
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

        unless ($self->_rate_ok($sess, 'publish', $self->publish_rate_limit)) {
            $reply_error->(ERR_PERMISSION_DENIED, 'publish rate limit exceeded');
            return;
        }

        my $tags = $cmd->{tags};

        if (defined $tags) {
            unless (ref $tags eq 'HASH'
                && !grep { ref $_ } values %$tags)
            {
                $reply_error->(ERR_BAD_PUBLISH, 'tags must be an object of scalars');
                return;
            }
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

    my $sess = $self->connections->{$id} or return;

    unless ($self->_rate_ok($sess, 'message', $self->message_rate_limit)) {
        $self->log->warn("Session $id: message rate limit exceeded, closing") if $self->log;
        eval { $tx->finish };
        $self->drop_session($id);
        return;
    }

    my $f = $self->proto->decode_frame($raw);

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
        my $exp = $sess->{identity}{raw}{exp} // 0;
        return $reply->({
            client  => $self->_sid($id),
            version => $VERSION,
            expires => $exp,
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

        # channel limit guard (Task 4)
        my $existing = $sess->{subscribed}{$ch} ? 1 : 0;
        my $count    = scalar keys %{ $sess->{subscribed} // {} };
        my $limit    = $self->_channel_opts($ch)->{client_channel_limit};
        if (!$existing && $count >= $limit) {
            return $error->(ERR_BAD_SUBSCRIBE, "channel limit exceeded ($limit)");
        }

        my $res = $self->subscribe($id, $ch, $p->{since}, {
            recover => $p->{recover},
            epoch   => $p->{epoch},
            offset  => $p->{offset},
        });
        return $reply->($res);
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

        my $tags = $p->{tags};
        if (defined $tags) {
            return $error->(ERR_BAD_PUBLISH, 'tags must be an object')
                unless ref $tags eq 'HASH';
            for my $v (values %$tags) {
                return $error->(ERR_BAD_PUBLISH, 'tags values must be scalars')
                    if ref $v;
            }
        }

        return $error->(ERR_PERMISSION_DENIED, "not authorized to publish to '$ch'")
            unless $self->can_publish($id, $ch);
        my $seq = eval { $self->publish($ch, $p->{data}, $tags) };

        unless ($self->_rate_ok($sess, 'publish', $self->publish_rate_limit)) {
            return $error->(ERR_PERMISSION_DENIED, 'publish rate limit exceeded');
        }

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
        $self->_schedule_expiry($id);       # re-arm with the new exp
        return $reply->({
            client  => $self->_sid($id),
            expires => $new->{exp} // 0,
        });
    }

    return $error->(ERR_UNKNOWN_METHOD, "unknown method '$m'");
}

sub authorize_publish {
    my ($self, $identity, $channel) = @_;
    return $self->_allowed($identity, $channel, 'publish');
}

sub channel_stats {
    my ($self, $channel) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $info = $self->channel_info($channel);
    my $pres = $self->presence($channel);

    return {
        %$info,
        presence_users => scalar @$pres,
    };
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

sub _rate_ok {
    my ($self, $sess, $kind, $limit) = @_;
    return 1 unless $limit && $limit > 0;

    my $now = time;
    my $slot = $kind eq 'publish' ? 'pub' : 'msg';
    my $bucket = $sess->{rate}{$slot} //= { second => $now, count => 0 };

    if ($bucket->{second} != $now) {
        $bucket->{second} = $now;
        $bucket->{count}  = 0;
    }

    $bucket->{count}++;
    return $bucket->{count} <= $limit ? 1 : 0;
}

sub subscribe {
    my ($self, $id, $channel, $since, $opts) = @_;
    return unless $channel && length $channel;

    my $sess = $self->connections->{$id} or return;
    my $tx   = $sess->{tx};
    return unless $tx && !$tx->is_finished;

    my $co = $self->_channel_opts($channel);

    my $is_new = !$sess->{subscribed}{$channel};
    $sess->{subscribed}{$channel}++;
    $self->channels->{$channel}{$id} = $tx;
    $self->_ensure_subscribed($channel);

    if ($co->{presence}) {
        $self->redis->db->zadd(
            $self->_presence_key($channel), time, $self->_member($id, $sess->{user}),
        );
    }
    $self->_presence_event($channel, 'join', $id, $sess->{user}) if $is_new;

    $self->log->info("Session $id ($sess->{user}) subscribed to '$channel'")
        if $self->log;

    my $epoch = $self->_current_epoch($channel);

    if (!defined $opts) {
        if (defined $since && $since =~ /^\d+$/) {
            for my $m (@{ $self->history_for($channel, $since) }) {
                $tx->send($self->_wire_pub($channel, $m->{seq}, $m->{data}, $m->{tags}));
            }
        }
        return { channel => $channel, epoch => $epoch };
    }

    my $head        = $self->history_for($channel);
    my $head_offset = @$head ? $head->[-1]{seq} : 0;

    my $result = { channel => $channel, epoch => $epoch, offset => $head_offset };

    my $want_recover = $opts->{recover} ? 1 : 0;
    $want_recover = 1 if $co->{force_recover};

    my $client_epoch = $opts->{epoch};
    my $client_off   = $opts->{offset};
    $client_off = $since
        if !defined $client_off && defined $since && $since =~ /^\d+$/;

    if ($want_recover && $self->client_recover
        && defined $client_epoch && defined $client_off
        && $client_epoch eq $epoch
        && $client_off <= $head_offset)
    {
        my @missed   = grep { $_->{seq} > $client_off } @$head;
        my $earliest = @$head ? $head->[0]{seq} : 0;

        if (!@$head || $earliest <= $client_off + 1) {
            $result->{recovered}    = Mojo::JSON->true;
            $result->{publications} = [
                map { { offset => $_->{seq}, data => $_->{data},
                        (exists $_->{tags} ? (tags => $_->{tags}) : ()) } }
                @missed
            ];
            $result->{offset} = $head_offset;
            return $result;
        }
    }

    $result->{recovered} = Mojo::JSON->false;
    $result->{offset}    = 0 if !@$head;
    return $result;
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

    my $co = $self->_channel_opts($channel);
    if ($co->{presence}) {
        $self->redis->db->zrem(
            $self->_presence_key($channel),
            $self->_member($id, $sess->{user}),
        );
    }
    $self->_presence_event($channel, 'leave', $id, $sess->{user});

    $self->log->info("Session $id unsubscribed from '$channel'") if $self->log;
}

sub channel_info {
    my ($self, $channel) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $subs = $self->channels->{$channel} // {};
    my %users;
    for my $id (keys %$subs) {
        my $s = $self->connections->{$id} or next;
        $users{ $s->{user} // 'anonymous' }++;
    }

    my $h = $self->history_for($channel);

    return {
        channel        => $channel,
        num_clients    => scalar keys %$subs,
        num_users      => scalar keys %users,
        history_length => scalar @$h,
        last_offset    => @$h ? $h->[-1]{seq} : 0,
        epoch          => $self->_current_epoch($channel),
    };
}

sub history_remove {
    my ($self, $channel) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $hkey = $self->_history_key($channel);
    my $n = $self->redis->db->llen($hkey) // 0;
    $self->redis->db->del($hkey);
    $self->redis->db->del($self->_seq_key($channel));
    $self->redis->db->del($self->_epoch_key($channel));
    return $n;
}

sub revoke_user {
    my ($self, $user) = @_;
    return [] unless defined $user && length $user;

    my @kicked;
    for my $id (keys %{ $self->connections }) {
        my $s = $self->connections->{$id} or next;
        next unless ($s->{user} // '') eq $user;

        my $tx = $s->{tx};
        if ($tx && !$tx->is_finished) {
            my $frame = $self->protocol eq 'legacy'
                ? encode_json({ error => { code => ERR_TOKEN_EXPIRED, message => 'revoked' } })
                : $self->proto->error_frame(undef, ERR_TOKEN_EXPIRED, 'revoked');
            eval { $tx->send($frame) };
            eval { $tx->finish };
        }
        $self->drop_session($id);
        push @kicked, $id;
    }
    return \@kicked;
}

sub drop_session {
    my ($self, $id) = @_;
    my $sess = $self->connections->{$id} or return;
    delete $self->connections->{$id};
    Mojo::IOLoop->remove($sess->{ping_timer})   if $sess->{ping_timer};
    Mojo::IOLoop->remove($sess->{expiry_timer}) if $sess->{expiry_timer};

    for my $ch (keys %{ $sess->{subscribed} // {} }) {
        delete $self->channels->{$ch}{$id};
        delete $self->channels->{$ch}
            unless keys %{ $self->channels->{$ch} // {} };
        my $co = $self->_channel_opts($ch);
        if ($co->{presence}) {
            $self->redis->db->zrem(
                $self->_presence_key($ch),
                $self->_member($id, $sess->{user}),
            );
        }
        $self->_presence_event($ch, 'leave', $id, $sess->{user});
    }
}

sub publish {
    my ($self, $channel, $data, $tags) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $opts = $self->_channel_opts($channel);
    my $seq  = $self->redis->db->incr($self->_seq_key($channel));

    my $pub = {
        seq     => $seq,
        data    => $data,
        channel => $channel,
        node    => $self->_node_id,
    };
    $pub->{tags} = $tags if ref $tags eq 'HASH' && keys %$tags;
    my $redis_payload = encode_json($pub);

    my $hkey = $self->_history_key($channel);
    my $llen_before = $self->redis->db->llen($hkey) // 0;
    $self->redis->db->lpush($hkey, $redis_payload);
    $self->redis->db->ltrim($hkey, 0, $opts->{history_size} - 1);
    $self->redis->db->expire($hkey, $opts->{history_ttl})
        if $opts->{history_ttl} > 0;

    if ($llen_before >= $opts->{history_size}) {
        $self->_reset_epoch($channel);
    }

    $self->_local_send($channel, $self->_wire_pub($channel, $seq, $data, $tags));
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
            $self->_wire_pub(
                $m->{channel} // $channel,
                $m->{seq},
                $m->{data},
                $m->{tags},
            ),
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

    my $opts = $self->_channel_opts($channel);
    my $raw = $self->redis->db->lrange(
        $self->_history_key($channel), 0, $opts->{history_size} - 1
    ) // [];
    my @all = map { decode_json($_) } @$raw;
    @all = reverse @all;

    return [ @all ] unless defined $since && $since =~ /^\d+$/;
    return [ grep { $_->{seq} > $since } @all ];
}

sub presence {
    my ($self, $channel) = @_;
    $channel = 'default' unless defined $channel && length $channel;

    my $opts = $self->_channel_opts($channel);
    return [] unless $opts->{presence};   # presence disabled for this namespace

    my $key = $self->_presence_key($channel);
    $self->redis->db->zremrangebyscore($key, '-inf', time - $opts->{presence_ttl})
        if $opts->{presence_ttl} > 0;

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
            namespace   => $self->_namespace_of($ch),
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

sub _schedule_expiry {
    my ($self, $id) = @_;
    my $sess = $self->connections->{$id} or return;

    Mojo::IOLoop->remove($sess->{expiry_timer}) if $sess->{expiry_timer};

    my $exp = $sess->{identity}{raw}{exp} // 0;
    return unless $exp > 0;

    my $now  = time;
    my $wait = $exp - $now;

    if ($wait <= 0) {
        # Already expired: fire on next tick
        $wait = 0.01;
    }

    $sess->{expiry_timer} = Mojo::IOLoop->timer($wait => sub {
        my $s = $self->connections->{$id} or return;
        my $cur_exp = $s->{identity}{raw}{exp} // 0;
        my $now2    = time;

        if ($cur_exp > $now2) {
            # Refreshed between arm and fire: re-arm
            $self->_schedule_expiry($id);
            return;
        }

        $self->log->warn("Session $id: token expired, closing") if $self->log;

        my $tx = $s->{tx};
        if ($tx && !$tx->is_finished) {
            my $frame = $self->protocol eq 'legacy'
                ? encode_json({ error => { code => ERR_TOKEN_EXPIRED, message => 'expired' } })
                : $self->proto->error_frame(undef, ERR_TOKEN_EXPIRED, 'expired');
            eval { $tx->send($frame) };
            eval { $tx->finish };
        }
        $self->drop_session($id);
    });
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
      namespaces => {
          chat => {
              history_size    => 200,
              history_ttl     => 3600,
              presence        => 1,
              presence_events => 1,
          },
      },
  );

  websocket '/ws' => sub {
      my $c = shift;
      my $identity = $hub->authenticate($c->param('token'))
          or return $c->render(status => 401, text => "unauthorized\n");
      $hub->add_connection(
          $c,
          $c->param('channel'),
          $c->param('since'),
          $identity,
      );
  };

=head1 DESCRIPTION

A Redis-backed realtime pub/sub server for Mojolicious, modeled on
L<Centrifugo|https://centrifugal.dev>.

Supported: client tokens with glob-based per-channel permissions,
WebSocket handshake with pre-upgrade C<401>, subscribe/unsubscribe,
per-channel history with C<since> replay and C<epoch>/C<recover>
recovery semantics, presence (snapshot + join/leave events), keepalive
ping/pong with timeout, client-side publish with per-channel
authorization, backend publish via API key, per-namespace configuration,
reserved C<#> and C<$user> channel prefixes, and multi-node fan-out via
Redis pub/sub.

Two wire formats are available:

=over 4

=item * C<'legacy'> (default) - the pre-0.21 envelope,
C<{channel, seq, data}> for pushes and C<{subscribe: "ch"}> for
commands.

=item * C<'centrifuge'> - the Centrifugo JSON protocol, via
L<Mojo::Centrifuge::Protocol>. Frames are
C<{id, method: {params}}> upstream and
C<{push: {...}}> / C<{id, result}> / C<{id, error}> downstream.

=back

=head1 PROTOCOL

=head2 Centrifugo frames

Client to server:

  { "id": N, "connect":     {} }
  { "id": N, "subscribe":   { "channel": "ch", "recover": true, "epoch": "...", "offset": N } }
  { "id": N, "unsubscribe": { "channel": "ch" } }
  { "id": N, "publish":     { "channel": "ch", "data": ..., "tags": { } } }
  { "id": N, "presence":    { "channel": "ch" } }
  { "id": N, "refresh":     { "token": "..." } }
  { "id": N, "ping":        {} }
  {}

An empty C<{}> is a pong and refreshes the keepalive timer.

Server to client:

  { "id": N, "result": { ... } }
  { "id": N, "error":  { "code": N, "message": "..." } }
  { "push": { "channel": "ch", "pub": { "offset": N, "data": ..., "tags": { } } } }
  { "push": { "channel": "ch", "join":  { "client": "...", "user": "..." } } }
  { "push": { "channel": "ch", "leave": { "client": "...", "user": "..." } } }
  {}

=head2 Subscribe result

  {
    "channel": "ch",
    "epoch":   "1700000000-1234-5678",
    "offset":  N,
    "recovered": true | false,
    "publications": [ { "offset": N, "data": ..., "tags": { } }, ... ]   # only when recovered
  }

C<epoch> identifies one continuous history of the channel. It rotates
when history is trimmed. C<offset> is the current head. On a fresh
subscribe (no C<recover>), C<recovered> is false and C<publications>
is absent. With C<recover: true>, C<epoch> and C<offset> matching the
current state, and the gap still inside the retained window, the server
returns C<recovered: true> with the missed publications inline.

C<connect> result carries C<expires> as epoch seconds (C<0> = never),
C<ping> and C<pong> timeouts, and C<server> node id. When C<expires>
passes without a successful C<refresh>, the server sends C<109> and
closes.

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

Maximum number of publications retained per channel. Default C<100>.
Overridable per-namespace.

=head2 history_ttl

Seconds after which the history key of a channel expires. C<0> (the
default) disables expiry. When positive, the TTL is refreshed on every
publish. Overridable per-namespace.

=head2 ping_interval

Seconds between server pings. Default C<25>.

=head2 pong_timeout

Seconds the server waits for a pong after a ping before closing the
connection. Default C<10>.

=head2 presence_ttl

Seconds after which a presence entry is considered stale and dropped.
Default C<60>. Stale entries are removed lazily on C<presence> queries.
Overridable per-namespace.

=head2 protocol

C<'legacy'> (default) or C<'centrifuge'>. In C<'centrifuge'> mode the
WebSocket speaks the Centrifugo JSON protocol; in C<'legacy'> mode it
speaks the pre-0.21 envelope.

=head2 prefix

Redis key prefix. Default C<'centrifuge'>.

=head2 presence_events

When true (default), join/leave events are pushed to other subscribers
of the same channel. Overridable per-namespace.

=head2 namespaces

  my $hub = Mojo::Centrifuge->new(
      namespaces => {
          chat => {
              history_size         => 50,
              history_ttl          => 3600,
              presence             => 1,
              presence_ttl         => 60,
              presence_events      => 1,
              client_channel_limit => 128,
              force_recover        => 1,
              epoch_key_ttl        => 3600,
          },
          notifications => {
              history_size    => 20,
              history_ttl     => 300,
              presence        => 0,
              presence_events => 0,
          },
      },
  );

Per-namespace overrides. A channel's namespace is the substring before
the first C<:>. A channel without a colon belongs to the default
namespace C<''>, governed by the hub attributes alone.

Recognized keys: C<history_size>, C<history_ttl>, C<presence>,
C<presence_ttl>, C<presence_events>, C<client_channel_limit>,
C<force_recover>, C<epoch_key_ttl>. Absent keys inherit the hub value.

C<force_recover> - when true, recovery is attempted even if the client
sent C<recover: false>. Intended for namespaces where state continuity
is mandatory.

=head2 client_channel_limit

Maximum simultaneous subscriptions per WebSocket connection. Default
C<128>. Attempting to exceed returns C<201 bad subscribe>. Overridable
per-namespace.

=head2 publish_rate_limit

Maximum client-initiated publishes per second, per WebSocket
connection. Default C<0> (unlimited). Exceeding returns
C<103 permission denied> with message C<publish rate limit exceeded>.
Not applied to backend publishes via C<publish>.

=head2 message_rate_limit

Maximum inbound WebSocket frames per second, per connection, including
pings and pongs. Default C<0> (unlimited). Exceeding closes the
connection without a reply.

=head2 client_recover

When true (default), C<subscribe> with C<recover =E<gt> 1> honors
recovery. Set to false to force all clients into a fresh epoch on
reconnect (useful during incidents).

=head2 epoch_key_ttl

Seconds after which a channel's epoch key expires. Default C<0>
(never). When positive, set it E<lt>= C<history_ttl> for the same
namespace or recovery will fail spuriously.

=head1 METHODS

L<Mojo::Centrifuge> inherits all methods from L<Mojo::Base> and
implements the following.

=head2 authenticate

  my $identity = $hub->authenticate($token);

Verifies a client token and returns an identity hashref, or C<undef>
on failure. Call this B<before> the WebSocket upgrade and render a
real C<401> on failure.

The identity hashref carries C<user>, C<bound_channel>, C<raw> (the
decoded token payload), and C<token>.

=head2 add_connection

  my $id = $hub->add_connection($c, $channel, $since, $identity);

Registers a WebSocket connection. C<$c> must be a
L<Mojolicious::Controller> in a websocket action. C<$channel> is an
optional auto-subscribe channel; C<$since> is an optional history
replay offset. Returns the numeric session id.

Schedules the keepalive timer and the token expiry timer. If
C<$channel> is defined and the identity is authorized for it, the
session subscribes immediately.

=head2 publish

  my $seq = $hub->publish($channel, $data);
  my $seq = $hub->publish($channel, $data, { source => 'api' });

Publishes C<$data> to C<$channel>. Returns the sequence number. The
optional third argument is a tags hashref (string-to-scalar map);
tags are stored in history and forwarded on the wire.

Bypasses permission checks - intended for backend code. Client publish
goes through C<can_publish>.

=head2 broadcast

  my $seq = $hub->broadcast($channel, $msg);

Alias for C<publish>.

=head2 subscribe

  my $result = $hub->subscribe($id, $channel, $since, \%opts);

Subscribes the given session to C<$channel>. Returns a hashref.

Called with three arguments, performs a legacy replay (sends each
retained publication C<seq E<gt> $since> as an individual push frame)
and returns C<{ channel, epoch }>.

Called with four arguments, takes C<\%opts> with C<recover>, C<epoch>,
and C<offset>, and returns a full result per the subscribe result
frame documented above.

=head2 unsubscribe

  $hub->unsubscribe($id, $channel);

Removes the session from C<$channel>.

=head2 history_for

  my $arrayref = $hub->history_for($channel, $since);

Returns retained publications of C<$channel> as an arrayref of
C<{seq, data, tags?}> hashrefs, newest last.

=head2 history_remove

  my $n = $hub->history_remove($channel);

Clears retained history, sequence counter, and epoch for C<$channel>.
Returns the number of removed history entries. Active subscribers are
unaffected; new subscribers see an empty channel and a fresh epoch.

=head2 presence

  my $arrayref = $hub->presence($channel);

Returns current members of C<$channel> as an arrayref of
C<{id, user}>. Entries older than C<presence_ttl> are dropped first.
Returns an empty arrayref when presence is disabled for the channel's
namespace.

=head2 channel_info

  my $info = $hub->channel_info($channel);

Returns C<{ channel, num_clients, num_users, history_length,
last_offset, epoch }>.

=head2 channel_stats

  my $info = $hub->channel_stats($channel);

Same as C<channel_info>, plus C<presence_users> - the count of distinct
presence entries currently registered for C<$channel> after stale
entries (older than C<presence_ttl>) are pruned. Returns C<undef> if
C<$channel> has no live subscribers and no history.

Used by operational dashboards and by C</metrics>.

=head2 authorize_publish

  my $ok = $hub->authorize_publish($identity, $channel);

Returns true if C<$identity> may publish to C<$channel>.

=head2 can_publish

  my $ok = $hub->can_publish($session_id, $channel);

Session-scoped variant of C<authorize_publish>.

=head2 revoke_user

  my $ids = $hub->revoke_user($user);

Sends error 109 to every session owned by C<$user> and closes them.
Clients are expected to reconnect with a fresh token. Returns an
arrayref of the closed session ids.

=head2 stats

  my $hashref = $hub->stats;

Returns counters: C<connections>, C<sessions>, C<channels>,
C<timestamp>, C<version>. Each channel entry includes C<namespace>,
C<connections>, C<ids>, C<last_seq>, and C<history_len>.

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
stops its ping and expiry timers, and forgets it. Called automatically
when the WebSocket finishes; public so tests and admin endpoints can
force-disconnect.

=head2 _channel_opts

  my $opts = $hub->_channel_opts($channel);

Returns the effective configuration for C<$channel> after namespace
overrides are applied.

=head1 AUTHORIZATION

Token permissions are evaluated with glob matching (C<*> and C<?>).
The fallback chain per operation is:

  <operation> (subscribe|publish) -> channels -> channel -> deny

Two channel prefixes override the normal pattern match:

=over 4

=item * C<#name> - server-only. Any client subscribe or publish attempt
is rejected with C<103 permission denied>. Backend code may still
call C<< $hub->publish('#name', ...) >> directly.

=item * C<$user> - user-private. Only sessions whose token C<user>
equals C<user> may subscribe. Publish authorization is unaffected.

=back

Namespace-scoped channels use a colon: C<chat:room1>. Patterns must
match the same separator: a token scoped with C<subscribe =E<gt>
['chat:*']> may subscribe to C<chat:room1> but not to C<room1>.

=head1 ERROR CODES

Imported from L<Mojo::Centrifuge::Protocol>:

  100  internal error
  101  unauthorized
  103  permission denied
  105  unknown method
  107  bad request
  108  not available
  109  token expired
  201  bad subscribe
  202  bad publish

=head1 MULTI-NODE

Each node runs its own WebSocket server and shares the same Redis. Pub
sub fan-out is via C<centrifuge:pubsub:E<lt>channelE<gt>>. Presence is
stored in C<centrifuge:presence:E<lt>channelE<gt>> as a ZSET scored by
join time; stale members are pruned lazily. Sequence numbers live in
C<centrifuge:seq:E<lt>channelE<gt>>; epochs in
C<centrifuge:epoch:E<lt>channelE<gt>>; history in
C<centrifuge:history:E<lt>channelE<gt>>.

Redis pub/sub drops messages on reconnect. At-least-once delivery
requires Redis Streams, deferred to Phase 2.

=head1 AUTHOR

Your Name <you@example.com>

=head1 LICENSE

Same as Perl itself (see the LICENSE file).

=head1 SEE ALSO

L<Mojo::Centrifuge::Auth>, L<Mojo::Centrifuge::Protocol>,
L<Mojo::Redis>, L<Mojolicious>.

=head1 SEE ALSO

L<Mojo::Centrifuge::Auth>, L<Mojo::Centrifuge::Protocol>,
L<Mojo::Redis>, L<Mojolicious>.

=for Pod::Coverage _allowed _channel_opts _current_epoch _match
    _namespace_of _new_epoch _reset_epoch _schedule_expiry
    _schedule_ping _wire_pub _wire_join_leave _ensure_subscribed
    _local_send _presence_event _sid _member _history_key _seq_key
    _pubsub_ch _presence_key _epoch_key _is_server_channel
    _is_user_channel _user_channel_of _valid_channel _error_payload
    _on_message_legacy _on_message_centrifuge _rate_ok

=cut

