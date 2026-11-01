use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use T;

my ($hub, $sched) = make_hub();
sub client { my ($user, %claims) = @_; my $c = T::Conn->new($hub); my $r = $c->connect(token($sched, sub => $user, %claims)); return ($c, $r) }

# ---- keepalive ------------------------------------------------------------------
{
    my ($c) = client('a');
    $sched->advance(24);
    is scalar($c->take), 0, 'no ping before the interval';
    $sched->advance(1);
    my @f = $c->take;
    is scalar(@f), 1, 'ping at 25s';
    is_deeply $f[0], {}, 'ping is an empty object';
    ok !$c->closed, 'still connected while waiting for the pong';
    $c->client->handle_frame('{}');
    $sched->advance(30);
    ok !$c->closed, 'pong keeps the connection alive past the pong timeout';
    $c->take;

    $c->client->closed;
}
{
    # any frame counts as liveness, not only the empty pong
    my ($c) = client('a2');
    $sched->advance(25);
    $c->take;
    $c->client->handle_frame('{"id":5}');    # client ping command instead of a pong
    $sched->advance(20);
    ok !$c->closed, 'any client frame cancels the pong deadline';
    $c->client->closed;
}
{
    my ($c) = client('b');
    $sched->advance(25);
    $c->take;
    $sched->advance(9);
    ok !$c->closed, 'not closed 9s after ping';
    $sched->advance(2);
    is $c->close_code, 3009, 'no pong within pong_timeout -> 3009';
    ok $c->close_info->{reconnect}, 'client may reconnect';
    is scalar(keys %{ $hub->{clients} }), 0, 'dead client removed from hub';
}

# ---- timers are cleaned up (no leaks) --------------------------------------------
{
    my $before = $sched->pending;
    my ($c) = client('leak', exp => $sched->now + 100);
    $c->cmd({ subscribe => { channel => 'private:x', token => token($sched, sub => 'leak', channel => 'private:x', exp => $sched->now + 100) } });
    $c->client->closed;
    is $sched->pending, $before, 'closing a client removes all of its timers';
}

# ---- connection expiry / refresh -----------------------------------------------
{
    my $now = $sched->now;
    my ($c, $r) = client('e', exp => $now + 100);
    is $r->{connect}{ttl}, 100, 'ttl reported';
    my $new = token($sched, sub => 'e', exp => $now + 1000);
    my $id = $c->cmd({ refresh => { token => $new } });
    my ($f) = grep { $_->{id} == $id } $c->take;
    ok $f->{refresh}{expires}, 'refresh reply says it expires';
    is $f->{refresh}{ttl}, 1000, 'with the new ttl';
    for (1 .. 40) { $sched->advance(5); $c->client->handle_frame('{}') }    # 200s, past the old exp + grace
    ok !$c->closed, 'refreshed connection survives its original expiry';
    $c->client->closed;
}
{
    my $now = $sched->now;
    my ($c) = client('f', exp => $now + 100);
    for (1 .. 24) { $sched->advance(5); $c->client->handle_frame('{}') }    # t = 120, inside the 25s grace
    ok !$c->closed, 'grace period after exp';
    for (1 .. 3) { $sched->advance(5); $c->client->handle_frame('{}') }     # t = 135 > 125
    is $c->close_code, 3003, 'expired and not refreshed -> closed with 3003';
    ok $c->close_info->{reconnect}, 'client will reconnect with a fresh token';
}
{
    my ($c) = client('g', exp => $sched->now + 100);
    $c->cmd({ refresh => { token => token($sched, sub => 'someone-else', exp => $sched->now + 500) } });
    is $c->close_code, 3500, 'refresh with a token for another user -> closed';
}
{
    my ($c) = client('h', exp => $sched->now + 100);
    $c->cmd({ refresh => { token => 'garbage' } });
    is $c->close_code, 3500, 'refresh with an invalid token -> closed';
}
{
    my ($c, $r) = client('i');
    ok !$r->{connect}{expires}, 'token without exp: no expiry';
    for (1 .. 100) { $sched->advance(5); $c->client->handle_frame('{}') }
    ok !$c->closed, 'and it never expires';
    $c->cmd({ refresh => { token => token($sched, sub => 'i', exp => $sched->now + 30) } }); $c->take;
    for (1 .. 20) { $sched->advance(5); $c->client->handle_frame('{}') }
    is $c->close_code, 3003, 'refresh can add an expiry';
}

# ---- subscription expiry ---------------------------------------------------------
{
    my $now = $sched->now;
    my ($c) = client('s');
    my $tok = token($sched, sub => 's', channel => 'private:s', exp => $now + 60);
    $c->cmd({ subscribe => { channel => 'private:s', token => $tok } });
    my $r = ($c->take)[0]{subscribe};
    ok $r->{expires} && $r->{ttl} == 60, 'subscription expiry reported';
    my $id = $c->cmd({ sub_refresh => { channel => 'private:s', token => token($sched, sub => 's', channel => 'private:s', exp => $now + 600) } });
    ok( (grep { $_->{id} == $id && $_->{sub_refresh}{ttl} == 600 } $c->take), 'sub_refresh extends it');
    for (1 .. 30) { $sched->advance(5); $c->client->handle_frame('{}') }    # 150s
    ok !$c->closed, 'refreshed subscription still valid';
    for (1 .. 100) { $sched->advance(5); $c->client->handle_frame('{}') }
    is $c->close_code, 3004, 'expired subscription -> closed with 3004';
}

# ---- limits -----------------------------------------------------------------------
{
    my ($c) = client('big');
    $c->client->handle_frame('{"id":1,"publish":{"channel":"news","data":"' . ('x' x 70_000) . '"}}');
    is $c->close_code, 3503, 'oversized frame closes the connection';
}

# ---- hub stop ---------------------------------------------------------------------
{
    my ($h, $sc) = make_hub();
    my $c = T::Conn->new($h);
    $c->connect(token($sc));
    $h->stop;
    is $c->close_code, 3001, 'stop() disconnects clients with 3001 (shutdown)';
}

done_testing;
