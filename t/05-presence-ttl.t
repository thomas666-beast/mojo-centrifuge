use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge;

use lib 'lib', 't/lib';
use Test::MockRedis;
my $redis = Test::MockRedis->new;

{
    package MockTx;
    sub new { bless { sent => [] }, shift }
    sub is_finished { 0 }
    sub send { push @{ $_[0]{sent} }, $_[1]; 1 }
    sub sent { $_[0]{sent} }
    sub on { 1 }
}

{
    package MockC;
    sub new { bless {}, shift }
    sub tx { $_[0]{tx} ||= MockTx->new }
    sub inactivity_timeout { 1 }
    sub on { 1 }
    sub rendered { 1 }
}

my $hub   = Mojo::Centrifuge->new(redis => $redis, presence_ttl => 60);

sub zadd_calls { grep { $_->[0] eq 'zadd' } @{ $redis->calls } }
sub zrem_calls { grep { $_->[0] eq 'zrem' } @{ $redis->calls } }

subtest 'subscribe adds to ZSET scored by current time' => sub {
    my $c  = MockC->new;
    my $id = $hub->add_connection($c, undef, undef, { user => 'alice', raw => {} });
    ok $id, 'got session id';

    $hub->subscribe($id, 'room', undef);

    my @zadds = zadd_calls();
    is scalar(@zadds), 1, 'one zadd';
    like $zadds[0][1], qr/:presence:room$/, 'presence key';
    is $zadds[0][2], time, 'score is current epoch';
    like $zadds[0][3], qr/:alice$/, 'member ends in :alice';
};

subtest 'presence() runs ZREMRANGEBYSCORE before ZRANGE' => sub {
    my $before = scalar @{ $redis->calls };
    my $list   = $hub->presence('room');

    my @after = @{ $redis->calls }[ $before .. $#{ $redis->calls } ];
    my ($first) = grep { $_->[0] =~ /^z/ } @after;
    is $first->[0], 'zremrangebyscore', 'cleanup first';

    my ($zr) = grep { $_->[0] eq 'zrange' } @after;
    ok $zr, 'zrange called';
};

subtest 'stale member is dropped' => sub {
    my $key = $hub->_presence_key('room');
    $redis->{zsets}{$key}{'old-node-1:bob'} = time - 120;
    $redis->{zsets}{$key}{'fresh-node-2:carol'} = time;

    my $list = $hub->presence('room');
    my @users = map { $_->{user} } @$list;
    ok !grep(/^bob$/, @users), 'stale bob dropped';
    ok  grep(/^carol$/, @users), 'fresh carol kept';
};

subtest 'unsubscribe removes from ZSET' => sub {
    my $c  = MockC->new;
    my $id = $hub->add_connection($c, undef, undef, { user => 'dave', raw => {} });
    $hub->subscribe($id, 'room2', undef);
    my $before = scalar zrem_calls();
    $hub->unsubscribe($id, 'room2');
    is scalar(zrem_calls()), $before + 1, 'zrem called once';
};

subtest 'drop_session removes from ZSET' => sub {
    my $c  = MockC->new;
    my $id = $hub->add_connection($c, undef, undef, { user => 'erin', raw => {} });
    $hub->subscribe($id, 'room3', undef);
    my $before = scalar zrem_calls();
    $hub->drop_session($id);
    ok scalar(zrem_calls()) > $before, 'zrem called';
};

done_testing;
