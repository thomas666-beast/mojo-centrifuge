use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge;

{
    package MockRedis;
    sub new { bless { calls => [] }, shift }
    sub db { shift }
    sub pubsub { shift }
    sub _rec { my ($s, $n, @a) = @_; push @{ $s->{calls} }, [ $n, @a ] }
    sub incr  { my ($s, $k) = @_; $s->_rec('incr', $k); 1 }
    sub lpush { my ($s, $k, $v) = @_; $s->_rec('lpush', $k, $v); 1 }
    sub ltrim { my ($s, $k, $a, $b) = @_; $s->_rec('ltrim', $k, $a, $b); 1 }
    sub expire { my ($s, $k, $n) = @_; $s->_rec('expire', $k, $n); 1 }
    sub lrange { [] }
    sub zadd { 1 } sub zrem { 1 } sub zremrangebyscore { 1 } sub zrange { [] }
    sub sadd { 1 } sub srem { 1 } sub smembers { [] }
    sub calls { $_[0]{calls} }

    sub listen { my ($self, $ch, $cb) = @_; $self->_rec('listen', $ch); return 1 }
    sub notify { my ($self, $ch, $msg) = @_; $self->_rec('notify', $ch, $msg); return 1 }
}

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
    my $redis = MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 0);
    $hub->publish('c1', 'x');
    is scalar(expire_calls($redis)), 0, 'no expire';
};

subtest 'EXPIRE once when history_ttl > 0' => sub {
    my $redis = MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 300);
    $hub->publish('c2', 'x');
    my @e = expire_calls($redis);
    is scalar(@e), 1, 'one expire';
    like $e[0][1], qr/:history:c2$/, 'history key';
    is $e[0][2], 300, 'ttl value';
};

subtest 'EXPIRE refreshed on every publish' => sub {
    my $redis = MockRedis->new;
    my $hub   = Mojo::Centrifuge->new(redis => $redis, history_ttl => 60);
    $hub->publish('c3', 'a');
    $hub->publish('c3', 'b');
    $hub->publish('c3', 'c');
    is scalar(expire_calls($redis)), 3, 'three expires';
};

done_testing;
