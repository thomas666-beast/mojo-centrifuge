#!/usr/bin/env perl
# Prints what your Mojo::Redis returns for the calls the Redis engine makes.
#   REDIS_URL=redis://127.0.0.1:6379/15 carton exec perl t/redis_smoke.pl
use strict; use warnings;
use lib 'lib';
use Data::Dumper; $Data::Dumper::Indent = 1; $Data::Dumper::Terse = 1;
use Mojo::Redis;
use Mojo::Centrifuge::Engine::Redis;

my $redis = Mojo::Redis->new($ENV{REDIS_URL} // 'redis://127.0.0.1:6379/15');
my $db = $redis->db;
print "Mojo::Redis $Mojo::Redis::VERSION\n";
print "db->can(eval): ", ($db->can('eval') ? 'yes' : 'no'), "   db->can(call): ", ($db->can('call') ? 'yes' : 'no'), "\n";
print "call(EVAL return string): ",  Dumper($db->call('EVAL', "return 'a b'", 0));
print "call(EVAL return table):  ",  Dumper($db->call('EVAL', "return {'x','y'}", 0));
{ my $scalar = $db->call('EVAL', "return {'x','y'}", 0); print "call(EVAL table) in scalar context: '$scalar'   <- this is why the engine avoids call()\n"; }
print "method eval(string):      ",  Dumper($db->can('eval') ? $db->eval("return 'a b'", 0) : 'n/a');

my $en = Mojo::Centrifuge::Engine::Redis->new(redis => $redis, prefix => "smoke$$");
my $H = { history_size => 3, history_ttl => 60, meta_ttl => 600 };
print "stream_info: ", join(' | ', $en->stream_info('c', $H)), "\n";
print "publish:     ", join(' | ', $en->publish('c', '{"a":1}', 'null', $H)), "\n";
print "history:     ", Dumper([ $en->history('c') ]);
$en->presence_add('c', 'cl-1', '{"client":"cl-1","user":"u"}', 30);
print "presence:    ", Dumper($en->presence('c'));
my $keys = $db->keys("smoke$$:*");    # method style: array reference in scalar context
$db->del(@$keys) if ref $keys eq 'ARRAY' && @$keys;
print "done\n";
