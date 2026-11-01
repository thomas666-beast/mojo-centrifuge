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
    sub new { bless {}, shift }
    sub is_finished { 0 }
    sub send { 1 }
    sub on { 1 }
}

{
    package MockC;
    sub new { bless {}, shift }
    sub tx { $_[0]{tx} ||= MockTx->new }
    sub inactivity_timeout { 1 }
    sub on { 1 }
}

sub expire_calls { grep { $_->[0] eq 'expire' } @{ $_[0]->calls } }

subtest 'no EXPIRE when history_ttl == 0' => sub {
    my $redis = Test::MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 0);
    $hub->publish('c1', 'x');
    is scalar(expire_calls($redis)), 0, 'no expire';
};

subtest 'EXPIRE once when history_ttl > 0' => sub {
    my $redis = Test::MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 300);
    $hub->publish('c2', 'x');
    my @e = expire_calls($redis);
    is scalar(@e), 1, 'one expire';
    like $e[0][1], qr/:history:c2$/, 'history key';
    is $e[0][2], 300, 'ttl value';
};

subtest 'EXPIRE refreshed on every publish' => sub {
    my $redis = Test::MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 60);
    $hub->publish('c3', 'a');
    $hub->publish('c3', 'b');
    $hub->publish('c3', 'c');
    is scalar(expire_calls($redis)), 3, 'three expires';
};

done_testing;
