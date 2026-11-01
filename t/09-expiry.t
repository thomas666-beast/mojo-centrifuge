use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Mojo::IOLoop;
use Mojo::JSON qw(decode_json encode_json);
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::Protocol qw(:codes);
use Test::MockRedis;

{
    package MockTx;
    sub new { bless { sent => [], finished => 0 }, shift }
    sub is_finished { $_[0]{finished} }
    sub send { push @{ $_[0]{sent} }, $_[1]; 1 }
    sub finish { $_[0]{finished} = 1 }
    sub sent { $_[0]{sent} }
    sub on { 1 }
}
{
    package MockC;
    sub new { bless { tx => MockTx->new }, shift }
    sub tx { $_[0]{tx} }
    sub inactivity_timeout { 1 }
    sub on { 1 }
    sub rendered { 1 }
}

my $redis = Test::MockRedis->new;
my $auth  = Mojo::Centrifuge::Auth->new(secret => 's');
my $hub   = Mojo::Centrifuge->new(
    redis    => $redis,
    auth     => $auth,
    protocol => 'centrifuge',
);

sub new_sess_with_exp {
    my ($exp) = @_;
    my $payload = {
        user => 'alice',
        iat  => time,
        exp  => $exp,
        subscribe => ['room/*'],
    };
    my $ident = { user => 'alice', raw => $payload, token => 'x' };
    my $c  = MockC->new;
    my $id = $hub->add_connection($c, undef, undef, $ident);
    return ($id, $c->tx);
}

subtest 'connect returns real exp' => sub {
    my $future = time + 3600;
    my ($id, $tx) = new_sess_with_exp($future);
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 1, connect => {} }));
    my $f = decode_json($tx->sent->[-1]);
    is $f->{result}{expires}, $future;
};

subtest 'connect with no exp returns 0' => sub {
    my $ident = { user => 'anon', raw => {}, token => 'x' };
    my $c  = MockC->new;
    my $id = $hub->add_connection($c, undef, undef, $ident);
    $hub->_on_message_centrifuge($id, $c->tx, encode_json({ id => 1, connect => {} }));
    my $f = decode_json($c->tx->sent->[-1]);
    is $f->{result}{expires}, 0;
};

subtest 'expired token closes connection with 109' => sub {
    my ($id, $tx) = new_sess_with_exp(time - 1);

    my $done = 0;
    Mojo::IOLoop->timer(0.3 => sub { $done = 1; Mojo::IOLoop->stop });
    Mojo::IOLoop->start;

    ok $done, 'loop ran';
    ok $tx->is_finished, 'tx finished';
    ok !exists $hub->connections->{$id}, 'session dropped';

    my ($err_frame) = grep { /"error"/ } @{ $tx->sent };
    ok $err_frame, 'error frame sent';
    my $f = decode_json($err_frame);
    is $f->{error}{code}, ERR_TOKEN_EXPIRED;
};

subtest 'refresh reschedules and prevents close' => sub {
    my ($id, $tx) = new_sess_with_exp(time - 1);

    # Refresh before the 0.01 timer fires — impossible in this setup because
    # add_connection arms the timer immediately. Instead: fresh session with
    # a short exp, refresh, then let it expire-check.
    my ($id2, $tx2) = new_sess_with_exp(time + 1);
    $hub->_on_message_centrifuge($id2, $tx2, encode_json({ id => 1, connect => {} }));

    my $new_exp = time + 3600;
    my $fresh_tok = $auth->make_token(user => 'alice', ttl => $new_exp - time);
    # Wait — make_token computes exp = now + ttl, not an absolute. Use ttl that
    # gives us ~3600.
    $fresh_tok = $auth->make_token(user => 'alice', ttl => 3600);

    $hub->_on_message_centrifuge($id2, $tx2,
        encode_json({ id => 2, refresh => { token => $fresh_tok } }));

    my $f = decode_json($tx2->sent->[-1]);
    is $f->{id}, 2;
    ok $f->{result}{expires} > time + 3000, 'exp refreshed';

    # Old session should still close
    my $done = 0;
    Mojo::IOLoop->timer(0.3 => sub { $done = 1; Mojo::IOLoop->stop });
    Mojo::IOLoop->start;
    ok $tx->is_finished, 'expired session closed';
    ok !$tx2->is_finished, 'refreshed session still alive';
};

done_testing;
