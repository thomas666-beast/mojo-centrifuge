use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Mojo::JSON qw(encode_json decode_json);
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

my $auth = Mojo::Centrifuge::Auth->new(secret => 's');

sub new_hub {
    my (%attr) = @_;
    return Mojo::Centrifuge->new(
        redis    => Test::MockRedis->new,
        auth     => $auth,
        protocol => 'centrifuge',
        %attr,
    );
}

sub new_sess {
    my ($hub, %raw) = @_;
    my $tok = $auth->make_token(user => 'alice', %raw);
    my $c   = MockC->new;
    my $id  = $hub->add_connection($c, undef, undef, $hub->authenticate($tok));
    return ($id, $c->tx);
}

subtest 'publish_rate_limit = 2 allows 2, denies 3rd' => sub {
    my $hub = new_hub(publish_rate_limit => 2);
    my ($id, $tx) = new_sess($hub, publish => ['room:*']);

    for my $i (1..2) {
        $hub->_on_message_centrifuge($id, $tx, encode_json({
            id => $i,
            publish => { channel => 'room:x', data => "m$i" },
        }));
        my $f = decode_json($tx->sent->[-1]);
        ok !exists $f->{error}, "publish $i ok";
    }

    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 3,
        publish => { channel => 'room:x', data => 'm3' },
    }));
    my $f = decode_json($tx->sent->[-1]);
    is $f->{error}{code}, ERR_PERMISSION_DENIED;
    like $f->{error}{message}, qr/rate limit/;
};

subtest 'publish_rate_limit = 0 disables' => sub {
    my $hub = new_hub(publish_rate_limit => 0);
    my ($id, $tx) = new_sess($hub, publish => ['room:*']);

    for my $i (1..10) {
        $hub->_on_message_centrifuge($id, $tx, encode_json({
            id => $i,
            publish => { channel => 'room:x', data => "m$i" },
        }));
    }
    my @errors = grep { defined $_->{error} } map { decode_json($_) } @{ $tx->sent };
    is scalar @errors, 0, 'no rate-limit errors';
};

subtest 'message_rate_limit = 3 closes on 4th' => sub {
    my $hub = new_hub(message_rate_limit => 3);
    my ($id, $tx) = new_sess($hub, subscribe => ['room:*']);

    for my $i (1..3) {
        $hub->_on_message_centrifuge($id, $tx, encode_json({
            id => $i,
            subscribe => { channel => 'room:x' },
        }));
    }
    ok !$tx->is_finished, 'alive at limit';

    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 4,
        subscribe => { channel => 'room:y' },
    }));
    ok $tx->is_finished, 'finished on 4th';
    ok !exists $hub->connections->{$id}, 'session dropped';
};

subtest 'message_rate_limit counts non-publish frames too' => sub {
    my $hub = new_hub(message_rate_limit => 2);
    my ($id, $tx) = new_sess($hub, subscribe => ['room:*']);

    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 1, subscribe => { channel => 'room:a' } }));
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 2, ping => {} }));
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 3, subscribe => { channel => 'room:b' } }));

    ok $tx->is_finished, 'closed on 3rd';
};

subtest 'legacy mode honors publish_rate_limit' => sub {
    my $hub = new_hub(publish_rate_limit => 1);
    $hub->protocol('legacy');
    my ($id, $tx) = new_sess($hub, publish => ['room:*']);

    $hub->_on_message_legacy($id, $tx, encode_json({
        publish => 'room:x', data => 'm1', id => 1,
    }));
    my $f1 = decode_json($tx->sent->[-1]);
    ok !exists $f1->{error}, 'first ok';

    $hub->_on_message_legacy($id, $tx, encode_json({
        publish => 'room:x', data => 'm2', id => 2,
    }));
    my $f2 = decode_json($tx->sent->[-1]);
    is $f2->{error}{code}, ERR_PERMISSION_DENIED;
};

done_testing;
