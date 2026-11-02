package Mojo::Centrifuge::Engine::Redis;
use strict;
use warnings;
use Encode ();
use Digest::SHA ();
use JSON::PP ();
use Mojo::Centrifuge::Util qw(decode_json pub_payload bus_name);

# Redis engine (Mojo::Redis). All multi-step operations run as Lua scripts so
# they are atomic across workers, and publications are PUBLISHed from *inside*
# the script, which guarantees that every node sees them in offset order.
#
# Keys (prefix defaults to "centrifuge"):
#   <p>:meta:<ch>   hash   e=epoch, s=last offset
#   <p>:hist:<ch>   list   newest first, JSON payloads
#   <p>:pz:<ch>     zset   client id -> expiry timestamp
#   <p>:ph:<ch>     hash   client id -> client info JSON
#   <p>:bus:<name>  pub/sub channels
#
# Redis calls are blocking (same as the earlier versions of this module).

my $PUBLISH = <<'LUA';
local epoch = redis.call('HGET', KEYS[1], 'e')
if not epoch then
  epoch = ARGV[1]
  redis.call('HSET', KEYS[1], 'e', epoch)
end
local seq = redis.call('HINCRBY', KEYS[1], 's', 1)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
local payload = '{"t":"pub","ch":' .. ARGV[2] .. ',"o":' .. string.format('%d', seq)
  .. ',"e":"' .. epoch .. '","d":' .. ARGV[3] .. ',"i":' .. ARGV[4] .. '}'
redis.call('LPUSH', KEYS[2], payload)
redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[5]) - 1)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[6]))
redis.call('PUBLISH', ARGV[8], payload)
return string.format('%d', seq) .. ' ' .. epoch
LUA

my $STREAM_INFO = <<'LUA';
redis.call('HSETNX', KEYS[1], 'e', ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
local m = redis.call('HMGET', KEYS[1], 'e', 's')
return (m[1] or '') .. ' ' .. (m[2] or '0')
LUA

my $HISTORY = <<'LUA';
local m = redis.call('HMGET', KEYS[1], 'e', 's')
local list = redis.call('LRANGE', KEYS[2], 0, -1)
return (m[1] or '') .. ' ' .. (m[2] or '0') .. '\n' .. table.concat(list, '\n')
LUA

my $PRESENCE_ADD = <<'LUA';
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
return 1
LUA

my $PRESENCE_REMOVE = <<'LUA';
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
LUA

my $PRESENCE = <<'LUA';
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[1])
if #expired > 0 then
  redis.call('ZREM', KEYS[1], unpack(expired))
  redis.call('HDEL', KEYS[2], unpack(expired))
end
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], '+inf')
if #ids == 0 then return '' end
local vals = redis.call('HMGET', KEYS[2], unpack(ids))
local out = {}
for i, id in ipairs(ids) do
  if vals[i] then table.insert(out, id .. ' ' .. vals[i]) end
end
return table.concat(out, '\n')
LUA

my $counter = 0;

sub new {
    my ($class, %a) = @_;
    die "redis (Mojo::Redis) required\n" unless $a{redis};
    return bless {
        redis     => $a{redis},
        prefix    => $a{prefix} // 'centrifuge',
        now       => $a{now} || sub { time },
        listeners => {},
    }, $class;
}

sub _b { return Encode::encode('UTF-8', "$_[0]") }                       # characters -> bytes
sub _c { return defined $_[0] ? Encode::decode('UTF-8', "$_[0]") : undef }    # bytes -> characters
sub _k { my ($s, $type, $ch) = @_; return "$s->{prefix}:$type:$ch" }
sub _call {
    my ($s, $cmd, @args) = @_;
    my $db = $s->{redis}->db;
    my @a  = map { _b($_) } @args;
    my $m  = lc $cmd;
    return $db->can($m) ? $db->$m(@a) : $db->call(uc $cmd, @a);
}

# Scripts return one string; anything else means the Redis client behaves differently than expected.
sub _str {
    my ($what, $r) = @_;
    die "redis engine: unexpected reply to $what: " . (ref $r ? ref $r : (defined $r ? "'$r'" : 'undef')) . "\n"
        if ref $r || !defined $r;
    return _c($r);
}
sub _epoch { return substr Digest::SHA::sha1_hex(join '-', $$, time, rand(), ++$counter), 0, 8 }

sub publish {
    my ($self, $ch, $data_json, $info_json, $o) = @_;
    $o ||= {};
    my $bus = $self->_k('bus', bus_name($ch));
    if (($o->{history_size} // 0) > 0 && ($o->{history_ttl} // 0) > 0) {
        my $r = $self->_call(
            'EVAL', $PUBLISH, 2, $self->_k('meta', $ch), $self->_k('hist', $ch),
            _epoch(), JSON::PP->new->allow_nonref->encode("$ch"), $data_json, $info_json // 'null',
            $o->{history_size}, $o->{history_ttl}, $o->{meta_ttl} || 30 * 86400, $bus,
        );
        my ($seq, $epoch) = split / /, _str(publish => $r), 2;
        die "redis engine: bad publish reply\n" unless defined $epoch && $seq =~ /\A\d+\z/;
        return ($seq + 0, $epoch);
    }
    $self->_call('PUBLISH', $bus, pub_payload($ch, 0, '', $data_json, $info_json));
    return (0, '');
}

sub stream_info {
    my ($self, $ch, $o) = @_;
    return (0, '') unless ($o->{history_size} // 0) > 0 && ($o->{history_ttl} // 0) > 0;
    my $r = $self->_call('EVAL', $STREAM_INFO, 1, $self->_k('meta', $ch), _epoch(), $o->{meta_ttl} || 30 * 86400);
    my ($epoch, $seq) = split / /, _str(stream_info => $r), 2;
    die "redis engine: bad stream_info reply\n" unless defined $seq && $seq =~ /\A\d+\z/;
    return ($seq + 0, $epoch);
}

sub history {
    my ($self, $ch) = @_;
    my $r = $self->_call('EVAL', $HISTORY, 2, $self->_k('meta', $ch), $self->_k('hist', $ch));
    my @lines = split /\n/, _str(history => $r);
    my ($epoch, $seq) = split / /, (shift(@lines) // ' 0'), 2;
    die "redis engine: bad history reply\n" unless defined $seq && $seq =~ /\A\d+\z/;
    my @pubs;
    for my $raw (reverse @lines) {    # list is newest first
        next unless length $raw;
        my $m = decode_json($raw);
        push @pubs, { offset => $m->{o} + 0, data => $m->{d}, (ref $m->{i} eq 'HASH' ? (info => $m->{i}) : ()) };
    }
    return (\@pubs, $seq + 0, $epoch);
}

sub history_remove {
    my ($self, $ch) = @_;
    $self->_call('DEL', $self->_k('hist', $ch));
    return 1;
}

sub presence_add {
    my ($self, $ch, $client, $info_json, $ttl) = @_;
    my $exp = $self->{now}->() + $ttl;
    $self->_call('EVAL', $PRESENCE_ADD, 2, $self->_k('pz', $ch), $self->_k('ph', $ch), $exp, $client, $info_json, $ttl * 3 < 60 ? 60 : $ttl * 3);
    return 1;
}

sub presence_remove {
    my ($self, $ch, $client) = @_;
    $self->_call('EVAL', $PRESENCE_REMOVE, 2, $self->_k('pz', $ch), $self->_k('ph', $ch), $client);
    return 1;
}

sub presence {
    my ($self, $ch) = @_;
    my $r = $self->_call('EVAL', $PRESENCE, 2, $self->_k('pz', $ch), $self->_k('ph', $ch), $self->{now}->());
    my %out;
    for my $line (split /\n/, _str(presence => $r)) {
        my ($id, $json) = split / /, $line, 2;
        next unless defined $json;
        $out{$id} = decode_json($json);
    }
    return \%out;
}

# ---- bus (Redis pub/sub) ---------------------------------------------------
sub bus_subscribe {
    my ($self, $name, $cb, $owner) = @_;
    my $wrapper = sub { my ($ps, $msg) = @_; $cb->($name, _c($msg)) };
    $self->{listeners}{$name} = $wrapper;
    $self->{redis}->pubsub->listen($self->_k('bus', $name) => $wrapper);
}

sub bus_unsubscribe {
    my ($self, $name, $owner) = @_;
    my $wrapper = delete $self->{listeners}{$name} or return;
    eval { $self->{redis}->pubsub->unlisten($self->_k('bus', $name) => $wrapper); 1 };
}

sub bus_publish {
    my ($self, $name, $json) = @_;
    $self->{redis}->pubsub->notify($self->_k('bus', $name) => _b($json));
}

1;
