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
    redis            => $redis,
    auth             => $auth,
    protocol         => 'centrifuge',
    history_size     => 3,
    client_recover   => 1,
);

sub new_sess {
    my (%raw) = @_;
    my $tok = $auth->make_token(user => 'alice', %raw);
    my $i   = $hub->authenticate($tok);
    my $c   = MockC->new;
    my $id  = $hub->add_connection($c, undef, undef, $i);
    return ($id, $c->tx);
}
sub last_frame { decode_json($_[0]->sent->[-1]) }

subtest 'subscribe returns epoch + offset, recovered=false on first sub' => sub {
    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 1, subscribe => { channel => 'room/a' } }));
    my $f = last_frame($tx);
    is $f->{result}{channel}, 'room/a';
    ok defined $f->{result}{epoch},   'epoch present';
    ok defined $f->{result}{offset},  'offset present';
    ok !$f->{result}{recovered},      'not recovered';
};

subtest 'recover with matching epoch returns publications' => sub {
    # publish 3 messages, capture epoch + first offset
    $hub->publish('room/b', 'm1');
    $hub->publish('room/b', 'm2');
    $hub->publish('room/b', 'm3');
    my $epoch = $hub->_current_epoch('room/b');

    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 2,
        subscribe => {
            channel => 'room/b',
            recover => Mojo::JSON->true,
            epoch   => $epoch,
            offset  => 1,        # had seen m1 (offset 1), wants m2, m3
        },
    }));
    my $f = last_frame($tx);
    ok $f->{result}{recovered}, 'recovered true';
    is scalar @{ $f->{result}{publications} }, 2, 'two missed publications';
    is $f->{result}{publications}[0]{data}, 'm2';
    is $f->{result}{publications}[1]{data}, 'm3';
    is $f->{result}{offset}, 3, 'head offset';
};

subtest 'recover with stale epoch returns recovered=false' => sub {
    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 3,
        subscribe => {
            channel => 'room/c',
            recover => Mojo::JSON->true,
            epoch   => 'nonsense',
            offset  => 1,
        },
    }));
    my $f = last_frame($tx);
    ok !$f->{result}{recovered}, 'recovered false';
    ok !exists $f->{result}{publications}, 'no publications';
};

subtest 'recover with offset past history returns recovered=false' => sub {
    # history_size=3, publish 5 → offsets 1,2,3 dropped, 4,5 kept
    $hub->publish('room/d', 'x1');
    $hub->publish('room/d', 'x2');
    $hub->publish('room/d', 'x3');
    $hub->publish('room/d', 'x4');
    $hub->publish('room/d', 'x5');
    my $epoch = $hub->_current_epoch('room/d');

    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 4,
        subscribe => {
            channel => 'room/d',
            recover => Mojo::JSON->true,
            epoch   => $epoch,
            offset  => 1,       # client saw offset 1 — but offset 2 is gone
        },
    }));
    my $f = last_frame($tx);
    ok !$f->{result}{recovered}, 'gap too large → not recovered';
};

subtest 'epoch rotates when history is trimmed' => sub {
    $hub->publish('room/e', 'a');
    $hub->publish('room/e', 'b');
    $hub->publish('room/e', 'c');
    my $epoch1 = $hub->_current_epoch('room/e');
    $hub->publish('room/e', 'd');   # this push trims 'a'
    my $epoch2 = $hub->_current_epoch('room/e');
    isnt $epoch1, $epoch2, 'epoch rolled';
};

subtest 'legacy 3-arg subscribe replays history as pushes' => sub {
    $hub->publish('room/f', 'legacy-1');
    $hub->publish('room/f', 'legacy-2');

    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->subscribe($id, 'room/f', 0);

    my @frames = map { decode_json($_) } @{ $tx->sent };
    is scalar(@frames), 2, 'two replay pushes';
    is $frames[0]{push}{channel},   'room/f';
    is $frames[0]{push}{pub}{data}, 'legacy-1';
    is $frames[1]{push}{pub}{data}, 'legacy-2';
    ok defined $frames[0]{push}{pub}{offset}, 'offset present';
};

subtest 'legacy protocol replay uses flat envelope' => sub {
    my $lh = Mojo::Centrifuge->new(
        redis        => $redis,
        auth         => $auth,
        protocol     => 'legacy',
        history_size => 100,
    );
    $lh->publish('room/g', 'l1');
    $lh->publish('room/g', 'l2');

    my $tok = $auth->make_token(user => 'u', subscribe => ['room/*']);
    my $c   = MockC->new;
    my $id  = $lh->add_connection($c, 'room/g', 0, $lh->authenticate($tok));
    my @f   = map { decode_json($_) } @{ $c->tx->sent };
    is scalar(@f), 2, 'two frames';
    is $f[0]{channel}, 'room/g';
    is $f[0]{data},    'l1';
    is $f[1]{data},    'l2';
};

subtest 'recover publications carry tags' => sub {
    $hub->publish('room/rec', 'r1', { n => 1 });
    $hub->publish('room/rec', 'r2', { n => 2 });
    my $epoch = $hub->_current_epoch('room/rec');

    my ($id, $tx) = new_sess(subscribe => ['room/*']);
    $hub->_on_message_centrifuge($id, $tx, encode_json({
        id => 99,
        subscribe => {
            channel => 'room/rec',
            recover => Mojo::JSON->true,
            epoch   => $epoch,
            offset  => 0,
        },
    }));
    my $f = last_frame($tx);
    ok $f->{result}{recovered}, 'recovered';
    is_deeply $f->{result}{publications}[0]{tags}, { n => 1 };
    is_deeply $f->{result}{publications}[1]{tags}, { n => 2 };
};

done_testing;
