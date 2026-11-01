use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Mojo::JSON qw(encode_json decode_json);
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Test::MockRedis;

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

my $auth = Mojo::Centrifuge::Auth->new(secret => 's');

sub new_hub {
    my (%ns) = @_;
    return Mojo::Centrifuge->new(
        redis                => Test::MockRedis->new,
        auth                 => $auth,
        protocol             => 'centrifuge',
        history_size         => 100,
        history_ttl          => 0,
        presence_ttl         => 60,
        client_channel_limit => 128,
        namespaces           => \%ns,
    );
}

sub new_sess {
    my ($hub, %raw) = @_;
    my $tok = $auth->make_token(user => 'alice', %raw);
    my $c   = MockC->new;
    my $id  = $hub->add_connection($c, undef, undef, $hub->authenticate($tok));
    return ($id, $c->tx);
}

subtest '_namespace_of splits on first colon' => sub {
    my $hub = new_hub();
    is $hub->_namespace_of('chat:room1'),        'chat';
    is $hub->_namespace_of('notifications:u42'), 'notifications';
    is $hub->_namespace_of('room1'),             '';
    is $hub->_namespace_of('chat:'),             'chat';
    is $hub->_namespace_of(''),                  '';
    is $hub->_namespace_of(undef),               '';
};

subtest '_channel_opts falls back to hub defaults' => sub {
    my $hub = new_hub();
    my $o   = $hub->_channel_opts('plain');
    is $o->{history_size},         100;
    is $o->{history_ttl},          0;
    is $o->{presence},             1;
    is $o->{presence_ttl},         60;
    is $o->{client_channel_limit}, 128;
};

subtest '_channel_opts overrides per-namespace' => sub {
    my $hub = new_hub(
        chat => {
            history_size    => 5,
            history_ttl     => 300,
            presence        => 0,
            presence_ttl    => 30,
            client_channel_limit => 3,
        },
    );
    my $o = $hub->_channel_opts('chat:room1');
    is $o->{history_size},         5;
    is $o->{history_ttl},          300;
    is $o->{presence},             0;
    is $o->{presence_ttl},         30;
    is $o->{client_channel_limit}, 3;
};

subtest 'history_size honored per namespace' => sub {
    my $hub = new_hub(chat => { history_size => 2 });
    for my $i (1..5) { $hub->publish('chat:x', "m$i") }
    my $h = $hub->history_for('chat:x');
    is scalar @$h, 2, 'trimmed to 2';
    is $h->[0]{data}, 'm4';
    is $h->[1]{data}, 'm5';
};

subtest 'presence disabled for namespace returns []' => sub {
    my $hub = new_hub(notif => { presence => 0 });
    my ($id, $tx) = new_sess($hub, subscribe => ['notif/*']);
    $hub->subscribe($id, 'notif:user1', undef);

    # No ZADD should have happened
    my @zadds = grep { $_->[0] eq 'zadd' } @{ $hub->redis->calls };
    is scalar @zadds, 0, 'no zadd';

    is_deeply $hub->presence('notif:user1'), [], 'presence empty';
};

subtest 'presence_events disabled for namespace' => sub {
    my $hub = new_hub(notif => { presence_events => 0 });
    my ($id, $tx) = new_sess($hub, subscribe => ['notif/*']);
    $hub->subscribe($id, 'notif:user1', undef);
    my @frames = @{ $tx->sent };
    # No join frames should have been sent to self
    is scalar(@frames), 0, 'no join frame for self';
    # And no presence notification on pubsub
    my @notifies = grep { $_->[0] eq 'notify' } @{ $hub->redis->calls };
    is scalar @notifies, 0, 'no presence notify';
};

subtest 'client_channel_limit honored per namespace' => sub {
    my $hub = new_hub(chat => { client_channel_limit => 1 });
    my ($id, $tx) = new_sess($hub, subscribe => ['chat:*']);

    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 1, subscribe => { channel => 'chat:a' } }));
    ok !decode_json($tx->sent->[-1])->{error}, 'first ok';

    $hub->_on_message_centrifuge($id, $tx,
        encode_json({ id => 2, subscribe => { channel => 'chat:b' } }));
    is decode_json($tx->sent->[-1])->{error}{code}, 201, 'second denied';
};

subtest 'history_ttl per namespace sets EXPIRE' => sub {
    my $hub = new_hub(chat => { history_ttl => 300 });
    $hub->publish('chat:x', 'm');
    my ($exp) = grep { $_->[0] eq 'expire' } @{ $hub->redis->calls };
    ok $exp, 'expire called';
    is $exp->[2], 300;
};

subtest 'default namespace unaffected by overrides' => sub {
    my $hub = new_hub(chat => { history_size => 2, history_ttl => 300 });
    $hub->publish('plain', 'm');
    my ($exp) = grep { $_->[0] eq 'expire' } @{ $hub->redis->calls };
    ok !$exp, 'default ns has history_ttl=0, no expire';
};

done_testing;
