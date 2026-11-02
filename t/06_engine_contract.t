use strict;
use warnings;
use utf8;
use Test::More;
use lib 'lib';
use Time::HiRes ();
use Mojo::Centrifuge::Engine::Memory;
use Mojo::Centrifuge::Util qw(encode_json decode_json bus_name);

# The same checks run against every engine. The Redis engine only runs when you
# ask for it:   TEST_REDIS=redis://127.0.0.1:6379/15 prove -l t/06_engine_contract.t
my @engines = ([ memory => sub { Mojo::Centrifuge::Engine::Memory->new } ]);
my $cleanup = sub { };
if ($ENV{TEST_REDIS}) {
    require Mojo::Redis;
    require Mojo::IOLoop;
    require Mojo::Centrifuge::Engine::Redis;
    my $prefix = "cftest$$";
    my $redis  = Mojo::Redis->new($ENV{TEST_REDIS});
    push @engines, [ redis => sub { Mojo::Centrifuge::Engine::Redis->new(redis => $redis, prefix => $prefix) } ];
    $cleanup = sub {
        my $db   = $redis->db;
        my $keys = $db->keys("$prefix:*");    # method style: arrayref in scalar context
        $db->del(@$keys) if ref $keys eq 'ARRAY' && @$keys;
    };
}

sub wait_for(&;$) {
    my ($cond, $secs) = @_;
    my $end = Time::HiRes::time() + ($secs // 5);
    while (!$cond->() && Time::HiRes::time() < $end) {
        if ($INC{'Mojo/IOLoop.pm'}) { Mojo::IOLoop->one_tick } else { last }
    }
    return $cond->();
}

my $H = { history_size => 3, history_ttl => 60, meta_ttl => 600 };

for my $e (@engines) {
    my ($name, $make) = @$e;
    my $en = $make->();
    note "engine: $name";

    # stream position exists before the first publication and stays valid
    my ($off, $epoch) = $en->stream_info('c1', $H);
    is $off, 0, "$name: new stream starts at 0";
    like $epoch, qr/\A[0-9a-f]{8}\z/, "$name: epoch assigned";
    my ($o1, $e1) = $en->publish('c1', '{"n":1}', 'null', $H);
    is $o1, 1, "$name: first offset is 1";
    is $e1, $epoch, "$name: epoch from stream_info is the epoch used by publish";
    my ($o2) = $en->publish('c1', '"two"', '{"user":"u"}', $H);
    is $o2, 2, "$name: offsets increase";

    my ($pubs, $top, $ep) = $en->history('c1');
    is $top, 2, "$name: history top";
    is $ep, $epoch, "$name: history epoch";
    is_deeply [ map { $_->{offset} } @$pubs ], [ 1, 2 ], "$name: oldest first";
    is_deeply $pubs->[0]{data}, { n => 1 }, "$name: structured data";
    is $pubs->[1]{data}, 'two', "$name: scalar data";
    is $pubs->[1]{info}{user}, 'u', "$name: info kept";
    ok !exists $pubs->[0]{info}, "$name: no info when none was given";
    ok !ref($pubs->[0]{offset}) && $pubs->[0]{offset} =~ /\A\d+\z/, "$name: offset is a plain number";

    $en->publish('c1', $_, 'null', $H) for 3 .. 6;
    ($pubs, $top) = $en->history('c1');
    is_deeply [ map { $_->{offset} } @$pubs ], [ 4, 5, 6 ], "$name: history trimmed to history_size";
    is $top, 6, "$name: offsets keep counting past the window";

    $en->history_remove('c1');
    ($pubs, $top) = $en->history('c1');
    is scalar(@$pubs), 0, "$name: history_remove empties the list";
    is $top, 6, "$name: but the position survives";
    is(($en->publish('c1', '7', 'null', $H))[0], 7, "$name: offsets continue after history_remove");

    my @np = $en->publish('c2', '1', 'null', {});
    is_deeply \@np, [ 0, '' ], "$name: channel without history has no offsets";
    is_deeply [ $en->stream_info('c2', {}) ], [ 0, '' ], "$name: and no stream position";

    # unicode
    $en->publish('c3', encode_json("gr\x{fc}\x{df}e \x{263A}"), 'null', $H);
    my ($c3) = $en->history('c3');
    is $c3->[0]{data}, "gr\x{fc}\x{df}e \x{263A}", "$name: unicode data survives";

    # presence
    $en->presence_add('p1', 'cl-1', encode_json({ client => 'cl-1', user => 'anna' }), 60);
    $en->presence_add('p1', 'cl-2', encode_json({ client => 'cl-2', user => "j\x{fc}rgen" }), 1);
    my $p = $en->presence('p1');
    is_deeply [ sort keys %$p ], [ 'cl-1', 'cl-2' ], "$name: presence lists clients";
    is $p->{'cl-2'}{user}, "j\x{fc}rgen", "$name: unicode user";
    $en->presence_add('p1', 'cl-1', encode_json({ client => 'cl-1', user => 'anna' }), 60);
    is scalar(keys %{ $en->presence('p1') }), 2, "$name: re-adding does not duplicate";
    $en->presence_remove('p1', 'cl-1');
    is_deeply [ keys %{ $en->presence('p1') } ], [ 'cl-2' ], "$name: remove";
    sleep 3;    # cl-2 had a 1s TTL (redis expiry uses whole seconds)
    is scalar(keys %{ $en->presence('p1') }), 0, "$name: entries expire without a heartbeat";
    is_deeply $en->presence('nothing'), {}, "$name: empty presence";

    # bus
    my @got;
    $en->bus_subscribe(bus_name('b1'), sub { push @got, $_[1] }, 'owner');
    wait_for { 1 } 0.3 if $name eq 'redis';    # let the subscription settle
    $en->publish('b1', '{"x":1}', '{"user":"u"}', $H) for 1 .. 20;
    ok wait_for { @got >= 20 }, "$name: bus delivered all 20";
    my @m = map { decode_json($_) } @got;
    is_deeply [ map { $_->{o} } @m[ 0 .. 19 ] ], [ 1 .. 20 ], "$name: bus messages arrive in offset order";
    is $m[0]{t}, 'pub', "$name: bus message type";
    is $m[0]{ch}, 'b1', "$name: bus message channel";
    my (undef, undef, $bus_epoch) = $en->history('b1');
    is $m[0]{e}, $bus_epoch, "$name: bus message carries the epoch";
    is_deeply $m[0]{d}, { x => 1 }, "$name: bus message data";
    is $m[0]{i}{user}, 'u', "$name: bus message info";

    $en->bus_publish(bus_name('b1'), '{"t":"join","ch":"b1"}');
    ok wait_for { @got >= 21 }, "$name: generic bus_publish";

    $en->bus_unsubscribe(bus_name('b1'), 'owner');
    wait_for { 1 } 0.3 if $name eq 'redis';
    my $n = @got;
    $en->publish('b1', '1', 'null', $H);
    wait_for { 0 } 0.5 if $name eq 'redis';
    is scalar(@got), $n, "$name: nothing after bus_unsubscribe";

    $cleanup->();
}

done_testing;
