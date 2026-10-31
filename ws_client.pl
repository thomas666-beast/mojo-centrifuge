use Mojo::Base -strict;
use Mojo::UserAgent;
use Mojo::IOLoop;
use Mojo::JSON qw(decode_json encode_json);

my $ua   = Mojo::UserAgent->new;
my $base = 'http://127.0.0.1:3000';
my $chan = 'news';

my $exit = 0;
my $last_seq = 0;
my $offline_started = 0;
my $presence_done = 0;
my $dyn_done = 0;
my $ping_done = 0;
my $cpub_done = 0;
my $pev_done  = 0;
my $api_done  = 0;
my (@live_a, @got_b);
my $tx_a;

Mojo::IOLoop->timer(60 => sub { fail('timeout') });

sub fail { say "!! FAIL: $_[0]"; $exit = 1; Mojo::IOLoop->stop }

sub err {
    my $e = $_[0]->error;
    return $e ? (($e->{code} // 'n/a') . " $e->{message}") : 'no response';
}

sub get_token {
    my ($user, $channel, $cb) = @_;
    $ua->get("$base/token?user=$user&channel=$channel" => sub {
        my ($ua, $tx) = @_;
        my $tok = $tx->res->body // '';
        chomp $tok;
        $cb->($tok);
    });
}

sub publish {
    my ($msg, $cb) = @_;
    get_token('publisher', $chan, sub {
        my $tok = shift;
        $ua->get("$base/publish?channel=$chan&msg=$msg&token=$tok" => sub {
            my ($ua, $tx) = @_;
            my $body = $tx->res->body // '(no body)';
            chomp $body;
            say "publish: $body";
            $cb->() if $cb;
        });
    });
}

sub publish_all {
    my ($msgs, $cb) = @_;
    my @todo = @$msgs;
    my $next;
    $next = sub {
        my $m = shift @todo;
        unless (defined $m) { undef $next; return $cb->() }
        publish($m, $next);
    };
    $next->();
}

sub show_presence {
    my ($label) = @_;
    $ua->get("$base/presence?channel=$chan" => sub {
        my ($ua, $tx) = @_;
        say "presence $label: " . $tx->res->body;
    });
}

# ---------------- Phase: keepalive ----------------
sub phase_ping {
    return if $ping_done++;
    say "\n=== keepalive phase (waits ~30s for a server ping) ===";

    get_token('erin', $chan, sub {
        my $tok = shift;
        $ua->websocket("$base/ws?channel=$chan&token=$tok" => sub {
            my ($ua, $tx) = @_;
            unless ($tx->is_websocket) { say "erin fail: " . err($tx); return }

            my $pings = 0;
            say "erin connected; waiting for server ping";

            $tx->on(message => sub {
                my $m = decode_json($_[1]);
                if ($m->{ping}) {
                    $pings++;
                    say "  [erin] got ping #$pings -> replying pong";
                    $tx->send(encode_json({ pong => 1 }));
                    return;
                }
                # app messages
                say "  [erin] channel=" . ($m->{channel} // '?') . " seq=" . ($m->{seq} // '?') . " " . ($m->{data} // '');
            });

            # Keep the connection open for ~30s (default ping_interval is 25s)
            Mojo::IOLoop->timer(35 => sub {
                if ($pings >= 1) {
                    say "\nPASS keepalive: received $pings server ping(s) in 35s";
                } else {
                    say "\nFAIL keepalive: got 0 server pings in 35s";
                    $exit = 1;
                }
                $tx->finish;
                Mojo::IOLoop->timer(0.3 => sub { Mojo::IOLoop->stop });
            });
        });
    });
}

# ---------------- Phase: client publish + hardening (Tasks 16/17) ----------------
sub get_token_q {
    my ($query, $cb) = @_;
    $ua->get("$base/token?$query" => sub {
        my ($ua, $tx) = @_;
        my $tok = $tx->res->body // '';
        chomp $tok;
        $cb->($tok);
    });
}

sub phase_clientpub {
    return if $cpub_done++;
    say "\n=== client publish phase (Tasks 16/17) ===";

    # Bad token must be rejected with a real HTTP 401 before the upgrade
    $ua->websocket("$base/ws?channel=$chan&token=garbage" => sub {
        my ($ua, $tx) = @_;
        my $code = $tx->res->code // 0;
        if (!$tx->is_websocket && $code == 401) { say "PASS 401: bad token rejected before upgrade" }
        else { say "FAIL 401: websocket=" . ($tx->is_websocket ? 1 : 0) . " code=$code"; $exit = 1 }
    });

    get_token('frank', $chan, sub {                        # rw: may publish (channel binding)
        my $tok_rw = shift;
        get_token_q("user=ro&subscribe=$chan", sub {       # ro: subscribe only, no publish
            my $tok_ro = shift;

            $ua->websocket("$base/ws?channel=$chan&token=$tok_rw" => sub {
                my ($ua, $rw) = @_;
                unless ($rw->is_websocket) { say "frank fail: " . err($rw); $exit = 1; return phase_ping() }

                $ua->websocket("$base/ws?channel=$chan&token=$tok_ro" => sub {
                    my ($ua, $ro) = @_;
                    unless ($ro->is_websocket) { say "ro fail: " . err($ro); $exit = 1; return phase_ping() }
                    say "frank (rw) and ro connected";

                    my (@rw_msgs, @ro_msgs);
                    $rw->on(message => sub { my $m = decode_json($_[1]); push @rw_msgs, $m unless $m->{ping} });
                    $ro->on(message => sub { my $m = decode_json($_[1]); push @ro_msgs, $m unless $m->{ping} });

                    Mojo::IOLoop->timer(0.3 => sub {
                        say "frank publishes via WS, ro tries to publish (denied), ro sends bad subscribe";
                        $rw->send(encode_json({ publish => $chan, data => 'from-ws' }));
                        $ro->send(encode_json({ publish => $chan, data => 'nope' }));
                        $ro->send(encode_json({ subscribe => { x => 1 } }));
                    });

                    Mojo::IOLoop->timer(1.2 => sub {
                        my $acked   = grep { defined $_->{published} } @rw_msgs;
                        my $ro_got  = grep { ($_->{data} // '') eq 'from-ws' } @ro_msgs;
                        my $leaked  = grep { ($_->{data} // '') eq 'nope' } (@rw_msgs, @ro_msgs);
                        my $denied  = grep { ref $_->{error} eq 'HASH' && ($_->{error}{message} // '') =~ /not authorized to publish/ } @ro_msgs;
                        my $invalid = grep { ref $_->{error} eq 'HASH' && ($_->{error}{message} // '') =~ /invalid channel/ } @ro_msgs;

                        my $ok = $acked && $ro_got && !$leaked && $denied && $invalid;
                        if ($ok) { say "\nPASS client publish: ack ok, ro received it, denied publish blocked, bad subscribe rejected" }
                        else {
                            say "\nFAIL client publish: acked=$acked ro_got=$ro_got leaked=$leaked denied=$denied invalid=$invalid";
                            $exit = 1;
                        }
                        $rw->finish; $ro->finish;
                        Mojo::IOLoop->timer(0.5 => sub { phase_presevents() });
                    });
                });
            });
        });
    });
}

# ---------------- Phase: presence join/leave events (Task 18) ----------------
sub phase_presevents {
    return if $pev_done++;
    say "\n=== presence events phase (Task 18) ===";

    get_token('pat', $chan, sub {
        my $tok_p = shift;
        get_token('pbo', $chan, sub {
            my $tok_b = shift;
            $ua->websocket("$base/ws?channel=$chan&token=$tok_p" => sub {
                my ($ua, $p) = @_;
                unless ($p->is_websocket) { say "pat fail: " . err($p); $exit = 1; return phase_ping() }

                my @pm;
                $p->on(message => sub { my $m = decode_json($_[1]); push @pm, $m unless $m->{ping} });
                say "pat connected, watching presence";

                Mojo::IOLoop->timer(0.3 => sub {
                    $ua->websocket("$base/ws?channel=$chan&token=$tok_b" => sub {
                        my ($ua, $b) = @_;
                        unless ($b->is_websocket) { say "pbo fail: " . err($b); $exit = 1; $p->finish; return phase_ping() }
                        say "pbo connected";

                        Mojo::IOLoop->timer(0.4 => sub { $p->send(encode_json({ presence => $chan })) });
                        Mojo::IOLoop->timer(0.8 => sub { say "pbo disconnects"; $b->finish });
                        Mojo::IOLoop->timer(1.5 => sub {
                            my $join      = grep { ($_->{event} // '') eq 'join'  && ($_->{user} // '') eq 'pbo' } @pm;
                            my $self_join = grep { ($_->{event} // '') eq 'join'  && ($_->{user} // '') eq 'pat' } @pm;
                            my $leave     = grep { ($_->{event} // '') eq 'leave' && ($_->{user} // '') eq 'pbo' } @pm;
                            my ($snap)    = grep { ref $_->{presence} eq 'ARRAY' } @pm;
                            my %seen      = map { ($_->{user} // '') => $_->{id} } @{ $snap ? $snap->{presence} : [] };
                            my $both      = $seen{pat} && $seen{pbo} && $seen{pat} ne $seen{pbo};

                            if ($join == 1 && !$self_join && $leave == 1 && $both) {
                                say "\nPASS presence events: got join + leave for pbo, no self-join, snapshot has unique ids";
                            } else {
                                say "\nFAIL presence events: join=$join self_join=$self_join leave=$leave snapshot_ok=" . ($both ? 1 : 0);
                                $exit = 1;
                            }
                            $p->finish;
                            Mojo::IOLoop->timer(0.3 => sub { phase_apikey() });
                        });
                    });
                });
            });
        });
    });
}

# ---------------- Phase: server API key + subscribe-since + request ids (Tasks 19/20) ----------------
my $API_KEY = $ENV{CENTRIFUGE_API_KEY} // 'dev-api-key-change-me';
my $room    = 'room';

sub api_publish {
    my ($data, $cb) = @_;
    $ua->post("$base/publish" => { 'X-API-Key' => $API_KEY } => json => { channel => $room, data => $data } => sub {
        my ($ua, $tx) = @_;
        $cb->($tx->res->code // 0, $tx->res->json // {});
    });
}

sub phase_apikey {
    return if $api_done++;
    say "\n=== server API key phase (Tasks 19/20) ===";
    my %r;

    get_token('sub1', $room, sub {
        my $tok1 = shift;
        $ua->websocket("$base/ws?token=$tok1" => sub {
            my ($ua, $s1) = @_;
            unless ($s1->is_websocket) { say "sub1 fail: " . err($s1); $exit = 1; return phase_ping() }

            my @m1;
            $s1->on(message => sub { my $m = decode_json($_[1]); push @m1, $m unless $m->{ping} });
            $s1->send(encode_json({ subscribe => $room }));

            Mojo::IOLoop->timer(0.3 => sub {
                api_publish({ n => 1 }, sub {
                    my ($code, $j) = @_;
                    my $seq1 = $j->{seq};
                    $r{api_publish} = ($code == 200 && defined $seq1);
                    api_publish({ n => 2 }, sub {
                        api_publish({ n => 3 }, sub {
                            Mojo::IOLoop->timer(0.4 => sub {
                                $r{api_live} = (grep { ref($_->{data}) eq 'HASH' } @m1) == 3;
                                $s1->finish;

                                # a fresh client asks for everything after seq1 via {subscribe, since}
                                get_token('sub2', $room, sub {
                                    my $tok2 = shift;
                                    $ua->websocket("$base/ws?token=$tok2" => sub {
                                        my ($ua, $s2) = @_;
                                        unless ($s2->is_websocket) { say "sub2 fail: " . err($s2); $exit = 1; return phase_ping() }

                                        my @m2;
                                        $s2->on(message => sub { my $m = decode_json($_[1]); push @m2, $m unless $m->{ping} });
                                        $s2->send(encode_json({ subscribe => $room, since => $seq1 }));

                                        Mojo::IOLoop->timer(0.5 => sub {
                                            my @rep = grep { ref($_->{data}) eq 'HASH' } @m2;
                                            $r{since_replay} = (@rep == 2 && $rep[0]{seq} == $seq1 + 1 && $rep[1]{seq} == $seq1 + 2);

                                            # replies echo the request id
                                            $s2->send(encode_json({ publish => $room, data => 'x', id => 'r1' }));
                                            $s2->send(encode_json({ presence => $room, id => 'r2' }));
                                        });
                                        Mojo::IOLoop->timer(1.0 => sub {
                                            $r{id_echo} = (grep { ($_->{id} // '') eq 'r1' && defined $_->{published} } @m2)
                                                       && (grep { ($_->{id} // '') eq 'r2' && ref $_->{presence} eq 'ARRAY' } @m2);
                                            $s2->finish;

                                            # wrong API key -> 401
                                            $ua->post("$base/publish" => { 'X-API-Key' => 'wrong' } => json => { channel => $room, data => 1 } => sub {
                                                my ($ua, $tx) = @_;
                                                $r{bad_key} = (($tx->res->code // 0) == 401);

                                                # client token scoped to another channel -> 403
                                                get_token('mallory', $chan, sub {
                                                    my $bad = shift;
                                                    $ua->post("$base/publish" => { Authorization => "Bearer $bad" } => json => { channel => $room, data => 1 } => sub {
                                                        my ($ua, $tx) = @_;
                                                        $r{wrong_scope} = (($tx->res->code // 0) == 403);

                                                        # client token for the right channel via Bearer header -> 200
                                                        get_token('rw', $room, sub {
                                                            my $good = shift;
                                                            $ua->post("$base/publish" => { Authorization => "Bearer $good" } => json => { channel => $room, data => 'ok' } => sub {
                                                                my ($ua, $tx) = @_;
                                                                $r{bearer_token} = (($tx->res->code // 0) == 200);

                                                                my @bad = grep { !$r{$_} } sort keys %r;
                                                                if (!@bad && keys %r == 7) {
                                                                    say "\nPASS api key: backend publish, live delivery, subscribe+since replay, id echo, 401/403, bearer token";
                                                                } else {
                                                                    say "\nFAIL api key: " . join(', ', map { "$_=" . ($r{$_} ? 1 : 0) } sort keys %r)
                                                                        . " (checks run: " . scalar(keys %r) . "/7)";
                                                                    $exit = 1;
                                                                }
                                                                Mojo::IOLoop->timer(0.3 => sub { phase_ping() });
                                                            });
                                                        });
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
}

# ---------------- existing phases ----------------
sub phase_dynamic {
    return if $dyn_done++;
    say "\n=== dynamic subscribe phase ===";
    get_token('dave', $chan, sub {
        my $tok = shift;
        $ua->websocket("$base/ws?token=$tok" => sub {
            my ($ua, $tx) = @_;
            unless ($tx->is_websocket) { say "dave fail: " . err($tx); return }
            say "dave connected (no channel)";

            my @recv;
            $tx->on(message => sub {
                my $m = decode_json($_[1]);
                # ignore keepalive frames
                return if $m->{ping};
                push @recv, $m;
                say "  [dave] channel=" . ($m->{channel} // '?') . " seq=" . ($m->{seq} // '?') . " " . ($m->{data} // '');
            });

            Mojo::IOLoop->timer(0.3 => sub {
                say "dave sends {subscribe: '$chan'}";
                $tx->send(encode_json({ subscribe => $chan }));
            });
            Mojo::IOLoop->timer(0.6 => sub {
                $ua->get("$base/presence?channel=$chan" => sub {
                    my ($ua, $p) = @_;
                    say "presence after dave subscribes: " . $p->res->body;
                });
            });
            Mojo::IOLoop->timer(0.9 => sub {
                get_token('publisher', $chan, sub {
                    my $t = shift;
                    $ua->get("$base/publish?channel=$chan&msg=m7&token=$t" => sub {
                        my ($ua, $r) = @_;
                        my $body = $r->res->body // '';
                        chomp $body;
                        say "publish m7: $body";
                    });
                });
            });
            Mojo::IOLoop->timer(1.4 => sub {
                say "dave sends {unsubscribe: '$chan'}";
                $tx->send(encode_json({ unsubscribe => $chan }));
            });
            Mojo::IOLoop->timer(1.7 => sub {
                get_token('publisher', $chan, sub {
                    my $t = shift;
                    $ua->get("$base/publish?channel=$chan&msg=m8&token=$t" => sub {
                        my ($ua, $r) = @_;
                        my $body = $r->res->body // '';
                        chomp $body;
                        say "publish m8: $body";
                    });
                });
            });
            Mojo::IOLoop->timer(2.2 => sub {
                my $got_m7 = grep { ($_->{data} // '') =~ /m7/ } @recv;
                my $got_m8 = grep { ($_->{data} // '') =~ /m8/ } @recv;
                if ($got_m7 && !$got_m8) {
                    say "\nPASS dynamic: received m7 (subscribed), skipped m8 (unsubscribed)";
                } else {
                    say "\nFAIL dynamic: m7=$got_m7 m8=$got_m8";
                    $exit = 1;
                }
                $tx->finish;
                Mojo::IOLoop->timer(0.3 => sub { phase_clientpub() });
            });
        });
    });
}

sub phase_presence {
    return if $presence_done++;
    say "\n=== presence: open alice + bob on '$chan' ===";
    get_token('alice', $chan, sub {
        my $tok_a = shift;
        get_token('bob', $chan, sub {
            my $tok_b = shift;
            $ua->websocket("$base/ws?channel=$chan&token=$tok_a" => sub {
                my ($ua, $txa) = @_;
                unless ($txa->is_websocket) { say "alice fail: " . err($txa); return }
                say "alice connected";
                $ua->websocket("$base/ws?channel=$chan&token=$tok_b" => sub {
                    my ($ua, $txb) = @_;
                    unless ($txb->is_websocket) { say "bob fail: " . err($txb); return }
                    say "bob connected";

                    Mojo::IOLoop->timer(0.4 => sub { show_presence('after both join') });
                    Mojo::IOLoop->timer(0.8 => sub { say "closing alice"; $txa->finish });
                    Mojo::IOLoop->timer(1.2 => sub { show_presence('after alice closes') });
                    Mojo::IOLoop->timer(1.6 => sub { say "closing bob"; $txb->finish });
                    Mojo::IOLoop->timer(2.0 => sub {
                        show_presence('after both close');
                        Mojo::IOLoop->timer(0.4 => sub { phase_dynamic() });
                    });
                });
            });
        });
    });
}

sub phase_reconnect {
    my $since = $last_seq;
    say "\n=== A reconnects with since=$since ===";
    get_token('alice', $chan, sub {
        my $tok = shift;
        $ua->websocket("$base/ws?channel=$chan&since=$since&token=$tok" => sub {
            my ($ua, $tx) = @_;
            return fail('reconnect failed: ' . err($tx)) unless $tx->is_websocket;
            say "A reconnected";

            $tx->on(message => sub {
                my $m = decode_json($_[1]);
                return if $m->{ping};
                push @got_b, $m->{seq};
                say "  [A replay/live] channel=" . ($m->{channel} // '?') . " seq=$m->{seq} $m->{data}";
            });

            Mojo::IOLoop->timer(0.5 => sub {
                say "\n=== live publish after reconnect ===";
                publish('m6', sub {
                    Mojo::IOLoop->timer(1.0 => sub {
                        $ua->get("$base/history?channel=$chan" => sub {
                            my ($ua, $h) = @_;
                            say "\n=== /history?channel=$chan ===";
                            say $h->res->body;

                            my $got = "@got_b";
                            my $want = join ' ', $since + 1 .. $since + 3;
                            if ($got eq $want) { say "\nPASS: received seq $got" }
                            else               { say "\nFAIL: got [$got], wanted [$want]"; $exit = 1 }

                            $tx->finish;
                            phase_presence();
                        });
                    });
                });
            });
        });
    });
}

sub phase_offline {
    return if $offline_started++;
    Mojo::IOLoop->timer(0.3 => sub {
        publish_all(['m4', 'm5'], \&phase_reconnect);
    });
}

get_token('alice', $chan, sub {
    my $tok = shift;
    $ua->websocket("$base/ws?channel=$chan&token=$tok" => sub {
        my ($ua, $tx) = @_;
        return fail('A handshake failed: ' . err($tx)) unless $tx->is_websocket;

        $tx_a = $tx;
        say "A connected (live)";

        $tx->on(message => sub {
            my $m = decode_json($_[1]);
            return if $m->{ping};
            $last_seq = $m->{seq};
            push @live_a, $m->{seq};
            say "  [A] channel=" . ($m->{channel} // '?') . " seq=$m->{seq} $m->{data}";
        });
        $tx->on(finish => sub { say "A closed"; phase_offline() });

        Mojo::IOLoop->timer(0.3 => sub {
            publish_all([map {"m$_"} 1 .. 3], sub {
                Mojo::IOLoop->timer(0.3 => sub {
                    say "\n=== A disconnects at last_seq=$last_seq ===";
                    $tx_a->finish;
                });
            });
        });
    });
});

Mojo::IOLoop->start;
exit $exit;
