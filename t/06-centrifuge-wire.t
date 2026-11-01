use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::JSON qw(decode_json encode_json);
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::Protocol qw(:codes);

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
    sub new { bless { tx => MockTx->new }, shift }
    sub tx { $_[0]{tx} }
    sub inactivity_timeout { 1 }
    sub on { 1 }
    sub rendered { 1 }
}

my $auth  = Mojo::Centrifuge::Auth->new(secret => 's');
my $hub   = Mojo::Centrifuge->new(
    redis    => $redis,
    auth     => $auth,
    protocol => 'centrifuge',
);

sub new_sess {
    my (%raw) = @_;
    my $tok = $auth->make_token(user => 'alice', %raw);
    my $i   = $hub->authenticate($tok);
    my $c   = MockC->new;
    my $id  = $hub->add_connection($c, undef, undef, $i);
    return ($id, $c->tx);
}

sub last_frame {
    my ($tx) = @_;
    return decode_json($tx->sent->[-1]);
}

subtest 'connect returns result with client, ping, pong' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 1, connect => {} }));
    my $f = last_frame($tx);
    is $f->{id}, 1;
    ok $f->{result}{client}, 'client';
    ok $f->{result}{version}, 'version';
    is $f->{result}{ping}, $hub->ping_interval;
    is $f->{result}{pong}, $hub->pong_timeout;
};

subtest 'subscribe unauthorized → 103' => sub {
    my ($id, $tx) = new_sess();   # no channel/subscribe patterns
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 2, subscribe => { channel => 'x' } }));
    my $f = last_frame($tx);
    is $f->{id}, 2;
    is $f->{error}{code}, ERR_PERMISSION_DENIED;
};

subtest 'subscribe authorized → result' => sub {
    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 3, subscribe => { channel => 'room/a' } }));
    my $f = last_frame($tx);
    is $f->{id}, 3;
    is $f->{result}{channel}, 'room/a';
};

subtest 'subscribe bad channel → 201' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 4, subscribe => { channel => { x => 1 } } }));
    my $f = last_frame($tx);
    is $f->{error}{code}, ERR_BAD_SUBSCRIBE;
};

subtest 'publish unauthorized → 103' => sub {
    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 5, publish => { channel => 'room/a', data => 'x' } }));
    my $f = last_frame($tx);
    is $f->{error}{code}, ERR_PERMISSION_DENIED;
};

subtest 'publish authorized → offset' => sub {
    my ($id, $tx) = new_sess(publish => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 6, publish => { channel => 'room/b', data => 'hi' } }));
    my $f = last_frame($tx);
    is $f->{id}, 6;
    ok defined $f->{result}{offset}, 'offset present';
    is $f->{result}{channel}, 'room/b';
};

subtest 'ping → {}' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 7, ping => {} }));
    is $tx->sent->[-1], '{}';
};

subtest 'pong clears awaiting_pong' => sub {
    my ($id, $tx) = new_sess();
    $hub->connections->{$id}{awaiting_pong} = 1;
    $hub->connections->{$id}{last_pong} = 1;
    $hub->_on_message_centrifuge($id, $tx, '{}');
    is $hub->connections->{$id}{awaiting_pong}, 0;
};

subtest 'empty {} is pong' => sub {
    my ($id, $tx) = new_sess();
    $hub->connections->{$id}{awaiting_pong} = 1;
    $hub->_on_message_centrifuge($id, $tx, '{}');
    is $hub->connections->{$id}{awaiting_pong}, 0;
};

subtest 'refresh with bad token → 109' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 8, refresh => { token => 'nope' } }));
    my $f = last_frame($tx);
    is $f->{error}{code}, ERR_TOKEN_EXPIRED;
};

subtest 'refresh with good token → result' => sub {
    my ($id, $tx) = new_sess();
    my $fresh = $auth->make_token(user => 'alice', ttl => 120);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 9, refresh => { token => $fresh } }));
    my $f = last_frame($tx);
    is $f->{id}, 9;
    ok $f->{result}{client}, 'client';
};

subtest 'unknown method → 105' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx, encode_json({ id => 10, nope => {} }));
    my $f = last_frame($tx);
    is $f->{error}{code}, ERR_UNKNOWN_METHOD;
};

subtest 'malformed json → 107' => sub {
    my ($id, $tx) = new_sess();
    $hub->_on_message_centrifuge($id, $tx, 'not json');
    my $f = last_frame($tx);
    is $f->{error}{code}, ERR_BAD_REQUEST;
};

subtest 'publish pushes wire-frame to subscriber' => sub {
    my $sub_tx = MockTx->new;
    my $sub_c  = MockC->new;
    $sub_c->{tx} = $sub_tx;
    my $sub_i = $hub->authenticate($auth->make_token(user => 'bob', subscribe => ['room/*']));
    my $sub_id = $hub->add_connection($sub_c, undef, undef, $sub_i);
    $hub->subscribe($sub_id, 'room/x', undef);

    $hub->publish('room/x', { hi => 1 });

    my ($push_raw) = grep { /"push"/ } @{ $sub_tx->sent };
    ok $push_raw, 'got push';
    my $f = decode_json($push_raw);
    is $f->{push}{channel}, 'room/x';
    is_deeply $f->{push}{pub}{data}, { hi => 1 };
    ok defined $f->{push}{pub}{offset}, 'offset present';
};

subtest 'publish with tags → push carries tags' => sub {
    my $sub_tx = MockTx->new;
    my $sub_c  = MockC->new;
    $sub_c->{tx} = $sub_tx;
    my $sub_i  = $hub->authenticate($auth->make_token(user => 'bob', subscribe => ['room/*']));
    my $sub_id = $hub->add_connection($sub_c, undef, undef, $sub_i);
    $hub->subscribe($sub_id, 'room/t', undef);

    $hub->publish('room/t', { hi => 1 }, { source => 'api', level => 'info' });

    my ($raw) = grep { /"push"/ } @{ $sub_tx->sent };
    my $f = decode_json($raw);
    is_deeply $f->{push}{pub}{tags}, { source => 'api', level => 'info' };
};

subtest 'publish without tags omits tags key' => sub {
    my $sub_tx = MockTx->new;
    my $sub_c  = MockC->new;
    $sub_c->{tx} = $sub_tx;
    my $sub_i  = $hub->authenticate($auth->make_token(user => 'bob', subscribe => ['room/*']));
    my $sub_id = $hub->add_connection($sub_c, undef, undef, $sub_i);
    $hub->subscribe($sub_id, 'room/u', undef);

    $hub->publish('room/u', { hi => 1 });

    my ($raw) = grep { /"push"/ } @{ $sub_tx->sent };
    my $f = decode_json($raw);
    ok !exists $f->{push}{pub}{tags}, 'no tags key';
};

subtest 'client publish with bad tags → 202' => sub {
    my ($id, $tx) = new_sess(publish => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 11,
        publish => { channel => 'room/x', data => 1, tags => [ 'not', 'hash' ] },
    }));
    my $f = decode_json($tx->sent->[-1]);
    is $f->{error}{code}, ERR_BAD_PUBLISH;
};

subtest 'client publish with nested tag → 202' => sub {
    my ($id, $tx) = new_sess(publish => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 12,
        publish => { channel => 'room/x', data => 1, tags => { a => { b => 1 } } },
    }));
    my $f = decode_json($tx->sent->[-1]);
    is $f->{error}{code}, ERR_BAD_PUBLISH;
};

subtest 'channel limit: N-th subscribe OK, N+1 → 201' => sub {
    my $hub2 = Mojo::Centrifuge->new(
        redis                => Test::MockRedis->new,
        auth                 => $auth,
        protocol             => 'centrifuge',
        client_channel_limit => 2,
    );
    my $tok = $auth->make_token(user => 'alice', subscribe => ['room/*']);
    my $c   = MockC->new;
    my $id  = $hub2->add_connection($c, undef, undef, $hub2->authenticate($tok));

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 1, subscribe => { channel => 'room/a' } }));
    is decode_json($c->tx->sent->[-1])->{result}{channel}, 'room/a';

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 2, subscribe => { channel => 'room/b' } }));
    is decode_json($c->tx->sent->[-1])->{result}{channel}, 'room/b';

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 3, subscribe => { channel => 'room/c' } }));
    my $f = decode_json($c->tx->sent->[-1]);
    is $f->{id}, 3;
    is $f->{error}{code}, ERR_BAD_SUBSCRIBE;
    like $f->{error}{message}, qr/channel limit exceeded/;
};

subtest 'channel limit: re-subscribe to existing channel OK at limit' => sub {
    my $hub2 = Mojo::Centrifuge->new(
        redis                => Test::MockRedis->new,
        auth                 => $auth,
        protocol             => 'centrifuge',
        client_channel_limit => 1,
    );
    my $tok = $auth->make_token(user => 'alice', subscribe => ['room/*']);
    my $c   = MockC->new;
    my $id  = $hub2->add_connection($c, undef, undef, $hub2->authenticate($tok));

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 1, subscribe => { channel => 'room/a' } }));

    # At limit (1). Re-subscribe to room/a should succeed.
    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 2, subscribe => { channel => 'room/a' } }));
    my $f = decode_json($c->tx->sent->[-1]);
    is $f->{id}, 2;
    is $f->{result}{channel}, 'room/a';
    ok !exists $f->{error}, 'no error';
};

subtest 'channel limit: unsubscribe frees a slot' => sub {
    my $hub2 = Mojo::Centrifuge->new(
        redis                => Test::MockRedis->new,
        auth                 => $auth,
        protocol             => 'centrifuge',
        client_channel_limit => 1,
    );
    my $tok = $auth->make_token(user => 'alice', subscribe => ['room/*']);
    my $c   = MockC->new;
    my $id  = $hub2->add_connection($c, undef, undef, $hub2->authenticate($tok));

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 1, subscribe => { channel => 'room/a' } }));

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 2, unsubscribe => { channel => 'room/a' } }));

    $hub2->_on_message_centrifuge($id, $c->tx,
        encode_json({ id => 3, subscribe => { channel => 'room/b' } }));
    my $f = decode_json($c->tx->sent->[-1]);
    is $f->{id}, 3;
    is $f->{result}{channel}, 'room/b';
    ok !exists $f->{error}, 'no error';
};

subtest 'channel limit: legacy envelope' => sub {
    my $hub2 = Mojo::Centrifuge->new(
        redis                => Test::MockRedis->new,
        auth                 => $auth,
        protocol             => 'legacy',
        client_channel_limit => 1,
    );
    my $tok = $auth->make_token(user => 'alice', subscribe => ['room/*']);
    my $c   = MockC->new;
    my $id  = $hub2->add_connection($c, undef, undef, $hub2->authenticate($tok));

    $hub2->_on_message_legacy($id, $c->tx,
        encode_json({ subscribe => 'room/a', id => 1 }));

    $hub2->_on_message_legacy($id, $c->tx,
        encode_json({ subscribe => 'room/b', id => 2 }));
    my $f = decode_json($c->tx->sent->[-1]);
    is $f->{id}, 2;
    is $f->{error}{code}, ERR_BAD_SUBSCRIBE;
};

subtest 'client publish to #server channel → 103' => sub {
    my ($id, $tx) = new_sess(publish => ['*'], subscribe => ['*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 50, publish => { channel => '#x', data => 1 } }));
    is decode_json($tx->sent->[-1])->{error}{code}, ERR_PERMISSION_DENIED;
};

subtest 'client subscribe to #server channel → 103' => sub {
    my ($id, $tx) = new_sess(subscribe => ['*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 51, subscribe => { channel => '#x' } }));
    is decode_json($tx->sent->[-1])->{error}{code}, ERR_PERMISSION_DENIED;
};

done_testing;
