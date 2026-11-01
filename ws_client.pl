#!/usr/bin/env perl
# Live end-to-end check against a running example.pl (needs Redis).
#   carton exec morbo example.pl      # or: perl example.pl daemon -l http://127.0.0.1:3000
#   carton exec perl ws_client.pl
# It speaks the Centrifugo JSON protocol by hand, so it also documents the wire format.
use Mojo::Base -strict;
use lib 'lib';
use Mojo::UserAgent;
use Mojo::IOLoop;
use Mojo::Promise;
use Mojo::URL;
use Scalar::Util qw(refaddr);
use Mojo::Centrifuge::Util qw(encode_json decode_json);

# ---------------------------------------------------------------------------
package Conn {
    use Mojo::Base -base;
    use Scalar::Util qw(refaddr);
    use Mojo::Centrifuge::Util qw(encode_json decode_json);

    has 'tx';
    has next_id => 1;
    has pending => sub { {} };
    has queue   => sub { [] };    # pushes nobody asked for yet
    has waiters => sub { [] };    # [predicate, promise, timer]
    has pings   => 0;
    has 'closed';
    has 'close_code';
    has close_p => sub { Mojo::Promise->new };

    sub dial {
        my ($class, $ua, $url) = @_;
        return $ua->websocket_p($url)->then(sub {
            my $tx   = shift;
            my $self = $class->new(tx => $tx);
            $tx->on(message => sub { $self->_message($_[1]) });
            $tx->on(finish  => sub {
                my ($tx, $code) = @_;
                $self->closed(1);
                $self->close_code($code);
                $self->_fail_all('connection closed');
                $self->close_p->resolve($code);
            });
            return $self;
        });
    }

    sub call {
        my ($self, $method, $params) = @_;
        my $p = Mojo::Promise->new;
        return $p->reject('connection closed') if $self->closed;
        my $id = $self->next_id;
        $self->next_id($id + 1);
        $self->pending->{$id} = $p;
        $self->tx->send(encode_json({ id => $id, $method => $params // {} }));
        return $p;
    }

    sub connect { my ($self, $token) = @_; return $self->call(connect => { token => $token }) }

    sub _message {
        my ($self, $text) = @_;
        for my $line (split /\n/, $text) {
            my $m = eval { decode_json($line) };
            next unless ref $m eq 'HASH';
            unless (%$m) {                       # server ping -> answer with an empty pong
                $self->pings($self->pings + 1);
                $self->tx->send('{}');
                next;
            }
            if (defined $m->{id}) {
                my $p = delete $self->pending->{ $m->{id} } or next;
                if   ($m->{error}) { $p->reject("error $m->{error}{code}: $m->{error}{message}") }
                else               { $p->resolve($m) }
                next;
            }
            $self->_push($m->{push}) if $m->{push};
        }
    }

    sub _push {
        my ($self, $push) = @_;
        my @w = @{ $self->waiters };
        for my $i (0 .. $#w) {
            next unless $w[$i][0]->($push);
            splice @{ $self->waiters }, $i, 1;
            Mojo::IOLoop->remove($w[$i][2]);
            return $w[$i][1]->resolve($push);
        }
        push @{ $self->queue }, $push;
    }

    # resolves with the first push matching $pred (already received or still to come)
    sub expect {
        my ($self, $pred, $secs) = @_;
        my @q = @{ $self->queue };
        for my $i (0 .. $#q) {
            next unless $pred->($q[$i]);
            splice @{ $self->queue }, $i, 1;
            return Mojo::Promise->resolve($q[$i]);
        }
        my $p = Mojo::Promise->new;
        my $timer = Mojo::IOLoop->timer(($secs // 5) => sub {
            @{ $self->waiters } = grep { refaddr($_->[1]) != refaddr($p) } @{ $self->waiters };
            $p->reject('timeout waiting for a push');
        });
        push @{ $self->waiters }, [ $pred, $p, $timer ];
        return $p;
    }

    sub _fail_all {
        my ($self, $why) = @_;
        my $pending = delete $self->{pending} // {};
        $self->pending({});
        $_->reject($why) for values %$pending;
        for my $w (splice @{ $self->waiters }) { Mojo::IOLoop->remove($w->[2]); $w->[1]->reject($why) }
    }
}
# ---------------------------------------------------------------------------
package main;

my $base    = $ENV{BASE}                // 'http://127.0.0.1:3000';
my $API_KEY = $ENV{CENTRIFUGE_API_KEY}  // 'dev-api-key-change-me';
(my $wsbase = $base) =~ s/^http/ws/;
my $WS = "$wsbase/connection/websocket";

my $ua = Mojo::UserAgent->new;
$ua->inactivity_timeout(120);

my ($pass, $fail) = (0, 0);
sub check {
    my ($ok, $name) = @_;
    $ok ? $pass++ : $fail++;
    say(($ok ? 'PASS' : 'FAIL') . " $name");
    return $ok;
}

sub sleep_p { my $p = Mojo::Promise->new; Mojo::IOLoop->timer($_[0] => sub { $p->resolve }); return $p }

sub token_p {
    my ($user, %q) = @_;
    my $url = Mojo::URL->new("$base/token")->query(user => $user, %q);
    return $ua->get_p($url)->then(sub { my $b = $_[0]->res->body // ''; chomp $b; $b });
}

sub sub_token_p {
    my ($user, $channel) = @_;
    my $url = Mojo::URL->new("$base/sub-token")->query(user => $user, channel => $channel);
    return $ua->get_p($url)->then(sub { my $b = $_[0]->res->body // ''; chomp $b; $b });
}

sub api_p {
    my ($method, $params, %headers) = @_;
    %headers = (Authorization => "apikey $API_KEY") unless %headers;
    return $ua->post_p("$base/api" => \%headers => json => { method => $method, params => $params })
        ->then(sub { return $_[0]->res });
}

# a connected client for $user
sub login_p {
    my $user = shift;
    return token_p($user)->then(sub {
        my $tok = shift;
        return Conn->dial($ua, $WS)->then(sub {
            my $c = shift;
            return $c->connect($tok)->then(sub { return $c });
        });
    });
}

my $ROOM  = "chat:room" . int(rand 1e6);    # fresh channel per run: offsets start at 1

# publish data {n => N} for each N, strictly one after the other (so offsets match N)
sub publish_seq_p {
    my @ns = @_;
    my $p  = Mojo::Promise->resolve;
    for my $n (@ns) {
        $p = $p->then(sub { api_p(publish => { channel => $ROOM, data => { n => $n } }) });
    }
    return $p;
}

my $state = {};

# ---- 1. connect, subscribe, live delivery -----------------------------------------
sub t_basic {
    say "\n=== connect + subscribe + live delivery ===";
    return login_p('anna')->then(sub {
        my $a = $state->{a} = shift;
        return $a->call(subscribe => { channel => $ROOM });
    })->then(sub {
        my $r = shift->{subscribe};
        check($r->{positioned} && $r->{recoverable}, 'channel is positioned + recoverable (history on)');
        check(length $r->{epoch}, 'epoch reported');
        $state->{epoch} = $r->{epoch};
        return publish_seq_p(1 .. 3);
    })->then(sub {
        return Mojo::Promise->all(map { $state->{a}->expect(sub { $_[0]{channel} eq $ROOM && $_[0]{pub} }) } 1 .. 3);
    })->then(sub {
        my @offs = sort { $a <=> $b } map { $_->[0]{pub}{offset} } @_;
        check("@offs" eq '1 2 3', "received offsets 1 2 3 (got @offs)");
        $state->{last} = $offs[-1];
    });
}

# ---- 2. disconnect, miss messages, recover them -------------------------------------
sub t_recovery {
    say "\n=== recovery after reconnect ===";
    $state->{a}->tx->finish;
    return $state->{a}->close_p->then(sub {
        return publish_seq_p(4 .. 5);
    })->then(sub { login_p('anna') })->then(sub {
        $state->{a} = shift;
        return $state->{a}->call(subscribe => { channel => $ROOM, recover => \1, offset => $state->{last}, epoch => $state->{epoch} });
    })->then(sub {
        my $r = shift->{subscribe};
        check($r->{recovered}, 'recovered');
        my @offs = map { $_->{offset} } @{ $r->{publications} // [] };
        check("@offs" eq '4 5', "got the 2 missed publications (offsets @offs)");
        check(($r->{publications}[0]{data}{n} // 0) == 4, 'with their data');
    });
}

# ---- 3. presence + join/leave -----------------------------------------------------------
sub t_presence {
    say "\n=== presence, join and leave ===";
    return login_p('bob')->then(sub {
        $state->{b} = shift;
        return $state->{b}->call(subscribe => { channel => $ROOM });
    })->then(sub {
        return $state->{a}->expect(sub { $_[0]{channel} eq $ROOM && $_[0]{join} });
    })->then(sub {
        check($_[0]{join}{info}{user} eq 'bob', 'anna saw bob join');
        return $state->{b}->call(presence => { channel => $ROOM });
    })->then(sub {
        my @users = sort map { $_->{user} } values %{ shift->{presence}{presence} };
        check("@users" eq 'anna bob', "presence lists both (@users)");
        return $state->{b}->call(presence_stats => { channel => $ROOM });
    })->then(sub {
        my $s = shift->{presence_stats};
        check($s->{num_clients} == 2 && $s->{num_users} == 2, 'presence_stats');
    });
}

# ---- 4. a client publishes -----------------------------------------------------------------
sub t_client_publish {
    say "\n=== client publish ===";
    return $state->{b}->call(publish => { channel => $ROOM, data => 'hello from bob' })->then(sub {
        return $state->{a}->expect(sub { $_[0]{channel} eq $ROOM && $_[0]{pub} && !ref $_[0]{pub}{data} });
    })->then(sub {
        my $pub = shift->{pub};
        check($pub->{data} eq 'hello from bob', 'anna received bob\'s message');
        check(($pub->{info}{user} // '') eq 'bob', 'with the sender info');
    })->then(sub {
        return $state->{b}->call(history => { channel => $ROOM, limit => 100 });
    })->then(sub {
        my $h = shift->{history};
        check(@{ $h->{publications} } >= 6 && $h->{offset} >= 6, 'history has all publications');
    })->then(sub {
        # leave
        $state->{b}->tx->finish;
        return $state->{a}->expect(sub { $_[0]{channel} eq $ROOM && $_[0]{leave} });
    })->then(sub {
        check($_[0]{leave}{info}{user} eq 'bob', 'anna saw bob leave');
    });
}

# ---- 5. security -----------------------------------------------------------------------------------
sub t_security {
    say "\n=== auth + permissions ===";
    my $bad;
    return Conn->dial($ua, $WS)->then(sub {
        $bad = shift;
        return $bad->connect('not.a.token')->catch(sub { 1 });
    })->then(sub { return $bad->close_p })->then(sub {
        check($_[0] == 3500, "bad token closes the socket with 3500 (got $_[0])");
        return $state->{a}->call(subscribe => { channel => 'private:vip' })->then(sub { check(0, 'private channel without token should fail') }, sub { check($_[0] =~ /error 103/, "private channel needs a token ($_[0])") });
    })->then(sub {
        return sub_token_p('anna', 'private:vip');
    })->then(sub {
        return $state->{a}->call(subscribe => { channel => 'private:vip', token => shift });
    })->then(sub {
        check(!$_[0]{error}, 'subscription token accepted');
        return $state->{a}->call(subscribe => { channel => 'nope:x' })->then(sub { check(0, 'unknown namespace should fail') }, sub { check($_[0] =~ /error 102/, "unknown namespace ($_[0])") });
    })->then(sub {
        return $state->{a}->call(subscribe => { channel => 'inbox:#bob' })->then(sub { check(0, 'foreign inbox should fail') }, sub { check($_[0] =~ /error 103/, "user-limited channel denied for other users ($_[0])") });
    })->then(sub {
        return $state->{a}->call(subscribe => { channel => 'inbox:#anna' });
    })->then(sub {
        check(!$_[0]{error}, 'user-limited channel allowed for its user');
        return $ua->post_p("$base/api" => json => { method => 'info', params => {} });
    })->then(sub {
        check($_[0]->res->code == 401, 'API without a key -> 401');
        return api_p(info => {}, Authorization => 'apikey wrong');
    })->then(sub {
        check($_[0]->code == 401, 'API with a wrong key -> 401');
        return api_p(info => {});
    })->then(sub {
        my $info = $_[0]->json;
        check($info->{result}{nodes}[0]{num_clients} >= 1, 'API info works with the key');
    });
}

# ---- 6. server API: disconnect ---------------------------------------------------------------------------
sub t_api_disconnect {
    say "\n=== server API: disconnect ===";
    return login_p('carol')->then(sub {
        $state->{c} = shift;
        return api_p(disconnect => { user => 'carol', disconnect => { code => 4100, reason => 'kicked' } });
    })->then(sub { return $state->{c}->close_p })->then(sub {
        check($_[0] == 4100, "API disconnect closed the socket with 4100 (got $_[0])");
    });
}

# ---- 7. keepalive --------------------------------------------------------------------------------------------
sub t_keepalive {
    say "\n=== keepalive (waits for a server ping, up to 30s) ===";
    my $p = Mojo::Promise->new;
    my $deadline = time + 30;
    my $iv; $iv = Mojo::IOLoop->recurring(0.5 => sub {
        return unless $state->{a}->pings > 0 || time > $deadline;
        Mojo::IOLoop->remove($iv);
        $p->resolve;
    });
    return $p->then(sub {
        check($state->{a}->pings > 0, 'server ping received and answered');
        return sleep_p(2);
    })->then(sub {
        check(!$state->{a}->closed, 'connection still open after the pong');
    });
}

Mojo::IOLoop->timer(120 => sub { say 'FAIL global timeout'; exit 1 });

t_basic()->then(\&t_recovery)->then(\&t_presence)->then(\&t_client_publish)
    ->then(\&t_security)->then(\&t_api_disconnect)->then(\&t_keepalive)
    ->catch(sub { check(0, "flow aborted: $_[0]") })
    ->finally(sub { Mojo::IOLoop->stop });

Mojo::IOLoop->start;
say "\n$pass passed, $fail failed";
exit($fail ? 1 : 0);
