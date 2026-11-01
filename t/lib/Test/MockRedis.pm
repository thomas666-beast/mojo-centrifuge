package Test::MockRedis;
use strict;
use warnings;

sub new {
    bless { calls => [], zsets => {}, lists => {}, kv => {}, seq => {} }, shift;
}

sub db     { shift }
sub pubsub { shift }

sub _rec { my ($s, $n, @a) = @_; push @{ $s->{calls} }, [ $n, @a ] }

# strings
sub get  { my ($s,$k)=@_; $s->_rec('get',$k); $s->{kv}{$k} }
sub set  { my ($s,$k,$v)=@_; $s->_rec('set',$k,$v); $s->{kv}{$k}=$v; 1 }
sub incr { my ($s,$k)=@_; my $n=($s->{seq}{$k}//0)+1; $s->{seq}{$k}=$n; $n }

# lists
sub lpush {
    my ($s,$k,$v)=@_;
    $s->_rec('lpush',$k,$v);
    unshift @{ $s->{lists}{$k} }, $v;
    1;
}
sub ltrim {
    my ($s,$k,$a,$b)=@_;
    $s->_rec('ltrim',$k,$a,$b);
    my $l = $s->{lists}{$k} // [];
    splice @$l, $b+1 if @$l > $b+1;
    1;
}
sub lrange {
    my ($s,$k,$a,$b)=@_;
    $s->_rec('lrange',$k,$a,$b);
    return [ @{ $s->{lists}{$k} // [] } ];
}
sub llen { my ($s,$k)=@_; scalar @{ $s->{lists}{$k} // [] } }
sub expire { my ($s,$k,$n)=@_; $s->_rec('expire',$k,$n); 1 }

# zsets
sub zadd {
    my ($s,$k,$sc,$m)=@_;
    $s->_rec('zadd',$k,$sc,$m);
    $s->{zsets}{$k}{$m}=$sc;
    1;
}
sub zrem {
    my ($s,$k,$m)=@_;
    $s->_rec('zrem',$k,$m);
    delete $s->{zsets}{$k}{$m};
    1;
}
sub zremrangebyscore {
    my ($s,$k,$min,$max)=@_;
    $s->_rec('zremrangebyscore',$k,$min,$max);
    for my $m (keys %{ $s->{zsets}{$k} // {} }) {
        my $sc = $s->{zsets}{$k}{$m};
        delete $s->{zsets}{$k}{$m} if $sc < $max;
    }
    1;
}
sub zrange {
    my ($s,$k,$a,$b)=@_;
    $s->_rec('zrange',$k,$a,$b);
    my @m = sort { $s->{zsets}{$k}{$a} <=> $s->{zsets}{$k}{$b} }
            keys %{ $s->{zsets}{$k} // {} };
    return \@m;
}

# sets
sub sadd { 1 } sub srem { 1 } sub smembers { [] }

# pubsub
sub listen { my ($s,$ch,$cb)=@_; $s->_rec('listen',$ch); 1 }
sub notify { my ($s,$ch,$m)=@_; $s->_rec('notify',$ch,$m); 1 }

sub calls { $_[0]{calls} }

1;
