use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use T;

my ($hub, $sched) = make_hub();
sub client { my ($user, $h) = @_; my $c = T::Conn->new($h || $hub); $c->connect(token($sched, sub => $user)); return $c }
sub sub_reply { my ($c, $ch, %p) = @_; $c->cmd({ subscribe => { channel => $ch, %p } }); return ($c->take)[0]{subscribe} }
sub pubs { my $c = shift; return map { $_->{push}{pub}{offset} } grep { $_->{push} && $_->{push}{pub} } $c->take }

# ---- recovery -------------------------------------------------------------------
my $pub = client('pub');
my $a = client('alice');
my $s = sub_reply($a, 'news');
my $epoch = $s->{epoch};
$pub->cmd({ publish => { channel => 'news', data => $_ } }) for 1 .. 3;
is_deeply [ pubs($a) ], [ 1, 2, 3 ], 'live delivery in order';
$a->client->closed;    # alice goes offline at offset 3
$pub->cmd({ publish => { channel => 'news', data => $_ } }) for 4 .. 5;

my $a2 = client('alice');
$s = sub_reply($a2, 'news', recover => \1, offset => 3, epoch => $epoch);
ok $s->{recovered}, 'recovered';
ok $s->{was_recovering}, 'was_recovering reported';
is_deeply [ map { $_->{offset} } @{ $s->{publications} } ], [ 4, 5 ], 'missed publications returned in order';
is_deeply [ map { $_->{data} } @{ $s->{publications} } ], [ 4, 5 ], 'with their data';
is $s->{offset}, 5, 'new position is the stream top';
$pub->cmd({ publish => { channel => 'news', data => 6 } });
is_deeply [ pubs($a2) ], [ 6 ], 'live messages continue after recovery, no duplicates';

# nothing missed
my $a3 = client('carol');
$s = sub_reply($a3, 'news', recover => \1, offset => 6, epoch => $epoch);
ok $s->{recovered} && !$s->{publications}, 'up to date: recovered with nothing to replay';

# wrong epoch -> not recovered (history was lost/restarted)
my $a4 = client('dave');
$s = sub_reply($a4, 'news', recover => \1, offset => 3, epoch => 'deadbeef');
ok $s->{was_recovering} && !$s->{recovered}, 'epoch mismatch -> not recovered';
is $s->{offset}, 6, 'client is told the current position';
isnt $s->{epoch}, 'deadbeef', 'and the current epoch';

# offset from the future -> not recovered
my $a5 = client('erin');
$s = sub_reply($a5, 'news', recover => \1, offset => 999, epoch => $epoch);
ok !$s->{recovered}, 'offset ahead of the stream -> not recovered';

# history window too small -> not recovered (messages fell out of history)
{
    my $p = client('p'); my $c = client('c');
    sub_reply($p, 'small:x'); 
    my $e = sub_reply($c, 'small:x')->{epoch};
    $c->client->closed;
    $p->cmd({ publish => { channel => 'small:x', data => $_ } }) for 1 .. 10;    # window is 3
    # this namespace does not allow client publish; publish via hub instead
}
{
    my $c = client('c');
    my $e = sub_reply($c, 'small:y')->{epoch};
    $c->client->closed;
    $hub->publish('small:y', $_) for 1 .. 10;
    my $c2 = client('c');
    my $r = sub_reply($c2, 'small:y', recover => \1, offset => 0, epoch => $e);
    ok !$r->{recovered}, 'messages that fell out of the history window -> not recovered';
    is $r->{offset}, 10, 'position still reported';
    my $c3 = client('c');
    $r = sub_reply($c3, 'small:y', recover => \1, offset => 8, epoch => $e);
    ok $r->{recovered} && @{ $r->{publications} } == 2, 'recent enough: recovered from the window';
}

# non-recoverable namespace ignores recover
{
    my $c = client('x');
    my $r = sub_reply($c, 'plain:z', recover => \1, offset => 1, epoch => 'x');
    ok !$r->{recoverable} && !$r->{positioned} && !$r->{was_recovering}, 'plain namespace: no recovery fields';
    ok !exists $r->{epoch}, 'no epoch without positioning';
}

# gap detection: a missed message forces a reconnect so the client can recover
{
    my $c = client('gap');
    sub_reply($c, 'news');
    my $top = $hub->publish('news', 'x')->{offset};
    $c->take;
    # simulate a lost bus message: deliver offset+2 directly
    my $ep = $c->client->{subs}{news}{epoch};
    $c->client->deliver_pub('news', $top + 2, $ep, '{"push":{}}');
    is $c->close_code, 3007, 'gap in offsets -> insufficient state (3007)';
    ok $c->close_info->{reconnect}, 'client is told to reconnect';
}
{
    my $c = client('dup');
    sub_reply($c, 'news');
    my $top = $hub->publish('news', 'y')->{offset};
    $c->take;
    $c->client->deliver_pub('news', $top, $c->client->{subs}{news}{epoch}, '{"push":{"dup":1}}');
    is scalar($c->take), 0, 'duplicate offset is ignored';
    $c->client->deliver_pub('news', $top + 1, 'otherepoch', '{"push":{}}');
    is $c->close_code, 3007, 'epoch change in a live message -> insufficient state';
}

# ---- history command ---------------------------------------------------------
{
    my $c = client('h');
    my $h = $hub->history('news', limit => 100);
    my $top = $h->{offset};
    $c->cmd({ history => { channel => 'news', limit => 2 } });
    my $r = ($c->take)[0]{history};
    is scalar(@{ $r->{publications} }), 2, 'history limit respected';
    is $r->{offset}, $top, 'history reports the stream top';
    $c->cmd({ history => { channel => 'news', limit => 0 } });
    is scalar(@{ ($c->take)[0]{history}{publications} }), 0, 'limit 0 returns only the position';
    $c->cmd({ history => { channel => 'news', limit => -1, reverse => \1 } });
    my @p = @{ ($c->take)[0]{history}{publications} };
    ok $p[0]{offset} > $p[-1]{offset}, 'reverse returns newest first';
    $c->cmd({ history => { channel => 'news', limit => 100, since => { offset => $top - 1, epoch => $r->{epoch} } } });
    is_deeply [ map { $_->{offset} } @{ ($c->take)[0]{history}{publications} } ], [ $top ], 'since offset';
    $c->cmd({ history => { channel => 'news', limit => 10, since => { offset => 1, epoch => 'wrong' } } });
    is(($c->take)[0]{error}{code}, 112, 'wrong epoch in history since -> 112');
    $c->cmd({ history => { channel => 'plain:z', limit => 1 } });
    is(($c->take)[0]{error}{code}, 108, 'history disabled -> 108');
}

# ---- presence ---------------------------------------------------------------
{
    my ($h, $sc) = make_hub(presence_ttl => 30);
    my $mk = sub { my $c = T::Conn->new($h); $c->connect(token($sc, sub => $_[0], info => { n => uc $_[0] })); $c };
    my ($x, $y) = ($mk->('xena'), $mk->('yuri'));
    $x->cmd({ subscribe => { channel => 'room' } }); $x->take;

    $y->cmd({ subscribe => { channel => 'room' } });
    my ($join) = grep { $_->{push} } $x->take;
    is $join->{push}{join}{info}{user}, 'yuri', 'existing subscriber sees the join';
    is $join->{push}{join}{info}{client}, $y->client->id, 'join carries the client id';
    is $join->{push}{join}{info}{conn_info}{n}, 'YURI', 'and the connection info from the token';
    is scalar(grep { $_->{push} } $y->take), 0, 'joiner does not get its own join push';

    $y->cmd({ presence => { channel => 'room' } });
    my $p = ($y->take)[0]{presence}{presence};
    is_deeply [ sort map { $_->{user} } values %$p ], [ 'xena', 'yuri' ], 'presence lists both';
    ok exists $p->{ $x->client->id }, 'keyed by client id';

    $y->cmd({ presence_stats => { channel => 'room' } });
    is_deeply(($y->take)[0]{presence_stats}, { num_clients => 2, num_users => 2 }, 'presence stats');

    my $z = $mk->('xena');    # same user, second connection
    $z->cmd({ subscribe => { channel => 'room' } }); $z->take; $x->take; $y->take;
    $z->cmd({ presence_stats => { channel => 'room' } });
    is_deeply(($z->take)[0]{presence_stats}, { num_clients => 3, num_users => 2 }, 'two connections of one user count once');

    $y->cmd({ unsubscribe => { channel => 'room' } });
    my ($leave) = grep { $_->{push} } $x->take;
    is $leave->{push}{leave}{info}{user}, 'yuri', 'leave push on unsubscribe';
    $z->client->closed;
    ($leave) = grep { $_->{push} } $x->take;
    is $leave->{push}{leave}{info}{client}, $z->client->id, 'leave push when the connection drops';
    is scalar(keys %{ $h->presence('room') }), 1, 'presence shrinks';

    # heartbeat keeps live clients, a crashed node's entries expire
    my $tick = sub { for (1 .. $_[0] / 5) { $sc->advance(5); $_->client->handle_frame('{}') for @_[1 .. $#_] } };
    $tick->(75, $x);    # x answers pings, so it stays connected
    is scalar(keys %{ $h->presence('room') }), 1, 'heartbeat refreshes presence of live clients past the TTL';
    $h->{engine}->presence_add('room', 'ghost-client', '{"client":"ghost-client","user":"ghost"}', 30);
    is scalar(keys %{ $h->presence('room') }), 2, 'a ghost entry is visible at first';
    $tick->(31, $x);
    is_deeply [ map { $_->{user} } values %{ $h->presence('room') } ], [ 'xena' ], 'ghost expired, live client kept';

    # presence permission
    my $q = $mk->('quinn');
    $q->cmd({ presence => { channel => 'private:r' } });
    is(($q->take)[0]{error}{code}, 103, 'presence not allowed for non-subscriber -> 103');
    $q->cmd({ presence => { channel => 'plain:r' } });
    is(($q->take)[0]{error}{code}, 108, 'presence disabled -> 108');
}

done_testing;
