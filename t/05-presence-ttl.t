use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge;

# --- minimal mock Redis: records ZADD/ZREM/ZREMRANGEBYSCORE/ZRANGE calls ---
{
    package MockRedis;
    sub new { bless { calls => [], zsets => {} }, shift }

    sub db { shift }
    sub pubsub { shift }

    sub _rec { my ($self, $name, @args) = @_; push @{ $self->{calls} }, [ $name, @args ] }

    sub zadd {
        my ($self, $key, $score, $member) = @_;
        $self->_rec('zadd', $key, $score, $member);
        $self->{zsets}{$key}{$member} = $score;
        return 1;
    }
    sub zrem {
        my ($self, $key, $member) = @_;
        $self->_rec('zrem', $key, $member);
        delete $self->{zsets}{$key}{$member};
        return 1;
    }
    sub zremrangebyscore {
        my ($self, $key, $min, $max) = @_;
        $self->_rec('zremrangebyscore', $key, $min, $max);
        for my $m (keys %{ $self->{zsets}{$key} // {} }) {
            my $s = $self->{zsets}{$key}{$m};
            delete $self->{zsets}{$key}{$m} if $s < $max;
        }
        return 1;
    }
    sub zrange {
        my ($self, $key, $start, $stop) = @_;
        $self->_rec('zrange', $key, $start, $stop);
        my @m = sort { $self->{zsets}{$key}{$a} <=> $self->{zsets}{$key}{$b} }
                keys %{ $self->{zsets}{$key} // {} };
        return \@m;
    }
    sub lpush { my ($s, $k, $v) = @_; $s->_rec('lpush', $k, $v); 1 }
    sub ltrim { my ($s, $k, $a, $b) = @_; $s->_rec('ltrim', $k, $a, $b); 1 }
    sub expire { my ($s, $k, $n) = @_; $s->_rec('expire', $k, $n); 1 }
    sub incr { my ($s, $k) = @_; $s->_rec('incr', $k); 1 }
    sub lrange { return [] }
    sub smembers { return [] }
    sub sadd { 1 }
    sub srem { 1 }

    sub calls { $_[0]{calls} }

    sub listen { my ($self, $ch, $cb) = @_; $self->_rec('listen', $ch); return 1 }
    sub notify { my ($self, $ch, $msg) = @_; $self->_rec('notify', $ch, $msg); return 1 }
}

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

my $redis = MockRedis->new;
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
