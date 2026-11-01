use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use T;
use Mojo::Centrifuge::Util qw(decode_json);

my ($hub, $sched) = make_hub();

# ---- connect ----------------------------------------------------------------
{
    my $c = T::Conn->new($hub);
    my $r = $c->connect(token($sched, sub => 'anna', exp => $sched->now + 3600, info => { name => 'Anna' }));
    ok $r->{connect}, 'connect reply';
    like $r->{connect}{client}, qr/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, 'client id is a uuid';
    is $r->{connect}{ping}, 25, 'ping interval announced';
    ok $r->{connect}{pong}, 'pong requested';
    ok $r->{connect}{expires}, 'expires flag set';
    is $r->{connect}{ttl}, 3600, 'ttl computed from exp';
    like $hub->{clients}{ $c->client->id} ? 'registered' : 'missing', qr/registered/, 'client registered in hub';
    is $c->client->user, 'anna', 'user from sub claim';
    $c->client->disconnect(3000, 'bye');
}

# ---- auth failures ------------------------------------------------------------
{
    my $c = T::Conn->new($hub);
    $c->cmd({ connect => { token => 'garbage' } });
    is $c->close_code, 3500, 'invalid token closes with 3500';
    ok !$c->close_info->{reconnect}, 'and tells the client not to reconnect';
}
{
    my $c = T::Conn->new($hub);
    my $id = $c->cmd({ connect => { token => token($sched, exp => $sched->now - 100) } });
    my ($f) = $c->take;
    is $f->{error}{code}, 109, 'expired token -> error 109';
    is $c->close_code, 3003, 'and a reconnectable close';
    ok $c->close_info->{reconnect}, 'reconnect=true so the client fetches a new token';
}
{
    my $c = T::Conn->new($hub);
    $c->cmd({ connect => {} });
    is $c->close_code, 3500, 'no token and anonymous disabled -> closed';
}
{
    my $h2 = make_hub(allow_anonymous => 1);
    my $c  = T::Conn->new($h2);
    my $r  = $c->connect(undef);
    ok $r->{connect}, 'anonymous connect works when enabled';
    is $c->client->user, '', 'anonymous user is empty string';
}
{
    my $c = T::Conn->new($hub);
    $c->cmd({ subscribe => { channel => 'news' } });
    is $c->close_code, 3501, 'first command must be connect';
}
{
    my $c = T::Conn->new($hub);
    $c->client->handle_frame('this is not json');
    is $c->close_code, 3501, 'garbage frame -> bad request';
}
{
    my $c = T::Conn->new($hub);
    $c->connect(token($sched));
    $c->cmd({ connect => {} });
    is $c->close_code, 3501, 'second connect -> bad request';
}
{
    my $c = T::Conn->new($hub);
    $sched->advance(11);
    is $c->close_code, 3502, 'no connect within connect_timeout -> stale';
}

# ---- subscribe + pubsub ---------------------------------------------------------
my ($a, $b) = map { T::Conn->new($hub) } 1 .. 2;
$a->connect(token($sched, sub => 'alice'));
$b->connect(token($sched, sub => 'bob'));

my $id = $a->cmd({ subscribe => { channel => 'news' } });
my ($f) = $a->take;
is $f->{id}, $id, 'reply carries the command id';
ok $f->{subscribe}{recoverable} && $f->{subscribe}{positioned}, 'recoverable + positioned reported';
is $f->{subscribe}{offset}, 0, 'new channel starts at offset 0';
like $f->{subscribe}{epoch}, qr/\A[0-9a-f]{8}\z/, 'epoch present';

$id = $a->cmd({ subscribe => { channel => 'news' } });
is(($a->take)[0]{error}{code}, 105, 'double subscribe -> 105');

$b->cmd({ subscribe => { channel => 'news' } });
$b->take; $a->take;    # drop reply + join push

my $pid = $b->cmd({ publish => { channel => 'news', data => { text => 'hi', n => 1 } } });
my @bf  = $b->take;
my ($ack) = grep { defined $_->{id} && $_->{id} == $pid } @bf;
ok $ack && !$ack->{error}, 'publish acknowledged';

my @af = $a->take;
is scalar(@af), 1, 'alice got exactly one frame';
is $af[0]{push}{channel}, 'news', 'push carries the channel';
is_deeply $af[0]{push}{pub}{data}, { text => 'hi', n => 1 }, 'data arrives intact';
is $af[0]{push}{pub}{offset}, 1, 'first publication has offset 1';
is $af[0]{push}{pub}{info}{user}, 'bob', 'client publications carry the sender info';
unlike $a->raw // '', qr/"offset":"/, 'offsets are JSON numbers, not strings';
my ($own) = grep { $_->{push} } @bf;
is $own->{push}{pub}{offset}, 1, 'publisher is subscribed, so it receives its own message';

# unicode survives the round trip
$b->cmd({ publish => { channel => 'news', data => "gr\x{fc}\x{df}e \x{1F600}" } });
@af = $a->take;
is $af[0]{push}{pub}{data}, "gr\x{fc}\x{df}e \x{1F600}", 'unicode data round trip';
$b->take;

# batching: two commands in one frame
my @ids = $b->cmd({ publish => { channel => 'news', data => 1 } }, { publish => { channel => 'news', data => 2 } });
my @r = grep { !$_->{push} } $b->take;
is_deeply [ map { $_->{id} } @r ], \@ids, 'two commands in one frame get two replies';
$a->take;

# unsubscribe stops delivery
$a->cmd({ unsubscribe => { channel => 'news' } });
is_deeply(($a->take)[0]{unsubscribe}, {}, 'unsubscribe acknowledged');
$b->cmd({ publish => { channel => 'news', data => 'after' } });
$b->take;
is scalar($a->take), 0, 'no delivery after unsubscribe';

# ---- permissions + errors ------------------------------------------------------
$a->cmd({ subscribe => { channel => 'private:vip' } });
is(($a->take)[0]{error}{code}, 103, 'private channel needs a token -> 103');
$a->cmd({ subscribe => { channel => 'nope:x' } });
is(($a->take)[0]{error}{code}, 102, 'unknown namespace -> 102');
$a->cmd({ subscribe => { channel => '' } });
is(($a->take)[0]{error}{code}, 107, 'empty channel -> 107');
$a->cmd({ subscribe => { channel => { x => 1 } } });
is(($a->take)[0]{error}{code}, 107, 'object as channel -> 107');
$a->cmd({ publish => { channel => 'news' } });
is(($a->take)[0]{error}{code}, 107, 'publish without data -> 107');
$a->cmd({ publish => { channel => 'inbox:x', data => 1 } });
is(($a->take)[0]{error}{code}, 103, 'publish not allowed -> 103');
$a->cmd({ frobnicate => { x => 1 } });
is(($a->take)[0]{error}{code}, 104, 'unknown method -> 104');
$a->cmd({ rpc => { method => 'x' } });
is(($a->take)[0]{error}{code}, 104, 'rpc without a handler -> 104');

# subscription token
my $stok = token($sched, sub => 'alice', channel => 'private:vip');
$a->cmd({ subscribe => { channel => 'private:vip', token => $stok } });
ok !(($a->take)[0]{error}), 'valid subscription token accepted';
$a->cmd({ unsubscribe => { channel => 'private:vip' } }); $a->take;
$a->cmd({ subscribe => { channel => 'private:vip', token => token($sched, sub => 'bob', channel => 'private:vip') } });
is(($a->take)[0]{error}{code}, 103, 'token for another user rejected');
$a->cmd({ subscribe => { channel => 'private:vip', token => token($sched, sub => 'alice', channel => 'private:other') } });
is(($a->take)[0]{error}{code}, 103, 'token for another channel rejected');
$a->cmd({ subscribe => { channel => 'private:vip', token => token($sched, sub => 'alice', channel => 'private:vip', exp => $sched->now - 60) } });
is(($a->take)[0]{error}{code}, 109, 'expired subscription token -> 109');

# publish for subscriber only
$a->cmd({ subscribe => { channel => 'private:vip', token => $stok } }); $a->take;
$a->cmd({ publish => { channel => 'private:vip', data => 'x' } });
ok !(($a->take)[0]{error}), 'subscriber may publish when allow_publish_for_subscriber';
$b->cmd({ publish => { channel => 'private:vip', data => 'x' } });
is(($b->take)[0]{error}{code}, 103, 'non-subscriber may not');

# user-limited channels
$a->cmd({ subscribe => { channel => 'inbox:#alice,carol' } });
ok !(($a->take)[0]{error}), 'user-limited channel allowed for a listed user';
$b->cmd({ subscribe => { channel => 'inbox:#alice,carol' } });
is(($b->take)[0]{error}{code}, 103, 'user-limited channel denied for others');

# client_channel_limit
{
    my $h = make_hub();
    $h->{client_channel_limit} = 2;
    my $c = T::Conn->new($h);
    $c->connect(token($sched));
    $c->cmd({ subscribe => { channel => "c$_" } }) for 1 .. 2;
    $c->take;
    $c->cmd({ subscribe => { channel => 'c3' } });
    is(($c->take)[0]{error}{code}, 106, 'channel limit -> 106');
}

# client ping command
$a->cmd({ id => 99 }) if 0;
$a->client->handle_frame('{"id":777}');
is_deeply [ $a->take ], [ { id => 777 } ], 'client ping gets an empty reply';

# connection closes cleanly: subscriptions removed from hub
my $before = scalar keys %{ $hub->{channels} };
$a->client->closed;
$b->client->closed;
is scalar(keys %{ $hub->{channels} }), 0, 'no channels left after both clients are gone';
is scalar(keys %{ $hub->{clients} }), 0, 'no clients left';
is scalar(keys %{ $hub->{users} }), 0, 'no users left';

done_testing;
