use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use T;
use Mojo::Centrifuge::Util qw(decode_json);

my ($hub, $sched, $engine) = make_hub();
sub client { my ($user, $h) = @_; my $c = T::Conn->new($h || $hub); $c->connect(token($sched, sub => $user)); return $c }
sub pushes { grep { $_->{push} } $_[0]->take }

# ---- server API -------------------------------------------------------------------
{
    my $c = client('api');
    $c->cmd({ subscribe => { channel => 'news' } }); $c->take;

    my $r = $hub->api('publish', { channel => 'news', data => { a => 1 } });
    ok $r->{result}{offset}, 'publish returns the offset';
    ok length $r->{result}{epoch}, 'and the epoch';
    my @p = pushes($c);
    is $p[0]{push}{pub}{data}{a}, 1, 'published data reaches subscribers';
    ok !exists $p[0]{push}{pub}{info}, 'server publications carry no client info';

    $r = $hub->api('publish', { channel => 'plain:x', data => 1 });
    is_deeply $r, { result => {} }, 'channel without history: empty result';

    $r = $hub->api('publish', { channel => 'news', data => 'skip', skip_history => \1 });
    is_deeply $r, { result => {} }, 'skip_history: no offset';
    @p = pushes($c);
    ok !exists $p[0]{push}{pub}{offset}, 'and the push has no offset';
    is $hub->history('news', limit => -1)->{publications}[-1]{data}{a}, 1, 'skipped message is not in history';

    is $hub->api('publish', { channel => 'news' })->{error}{code}, 107, 'publish needs data';
    is $hub->api('publish', { data => 1 })->{error}{code}, 107, 'publish needs a channel';
    is $hub->api('publish', { channel => 'nope:x', data => 1 })->{error}{code}, 102, 'unknown namespace';
    is $hub->api('bogus', {})->{error}{code}, 104, 'unknown method';
    is $hub->api(undef, {})->{error}{code}, 104, 'undefined method';

    $r = $hub->api('broadcast', { channels => [ 'news', 'plain:b', 'nope:c' ], data => 'all' });
    my @resp = @{ $r->{result}{responses} };
    ok $resp[0]{result}{offset}, 'broadcast: first ok with offset';
    is_deeply $resp[1], { result => {} }, 'broadcast: second ok';
    is $resp[2]{error}{code}, 102, 'broadcast: third failed on its own';
    is $hub->api('broadcast', { channels => [], data => 1 })->{error}{code}, 107, 'broadcast needs channels';

    $r = $hub->api('presence', { channel => 'news' });
    is scalar(keys %{ $r->{result}{presence} }), 1, 'presence via API';
    is_deeply $hub->api('presence_stats', { channel => 'news' })->{result}, { num_clients => 1, num_users => 1 }, 'presence_stats via API';
    is $hub->api('presence', { channel => 'plain:x' })->{error}{code}, 108, 'presence disabled -> 108';

    $r = $hub->api('history', { channel => 'news', limit => 100 });
    ok @{ $r->{result}{publications} } >= 1, 'history via API';
    $hub->api('history_remove', { channel => 'news' });
    is scalar(@{ $hub->api('history', { channel => 'news', limit => 100 })->{result}{publications} }), 0, 'history_remove';
    ok $hub->api('history', { channel => 'news' })->{result}{offset} > 0, 'but the stream position is kept';

    $r = $hub->api('channels', {});
    ok exists $r->{result}{channels}{news}, 'channels lists active channels';
    is_deeply [ keys %{ $hub->api('channels', { pattern => 'nope*' })->{result}{channels} } ], [], 'channels pattern';
    $r = $hub->api('info', {});
    is $r->{result}{nodes}[0]{num_clients}, 1, 'info';

    $r = $hub->api('batch', { commands => [ { method => 'publish', params => { channel => 'news', data => 1 } }, { method => 'nope' } ] });
    ok $r->{result}{replies}[0]{result}, 'batch: first reply';
    is $r->{result}{replies}[1]{error}{code}, 104, 'batch: second reply is an error';

    $c->take;
    $hub->api('unsubscribe', { user => 'api', channel => 'news' });
    my ($u) = pushes($c);
    is $u->{push}{unsubscribe}{code}, 2000, 'API unsubscribe pushes an unsubscribe';
    is_deeply [ $c->client->subscriptions ], [], 'and the client is no longer subscribed';

    $hub->api('disconnect', { user => 'api', disconnect => { code => 4100, reason => 'kicked' } });
    is $c->close_code, 4100, 'API disconnect closes with the given code';
    is $c->close_info->{reason}, 'kicked', 'and reason';
    ok $c->close_info->{reconnect}, '4100 is reconnectable';
}

# disconnect with whitelist / one client
{
    my ($a1, $a2) = (client('multi'), client('multi'));
    $hub->api('disconnect', { user => 'multi', whitelist => [ $a1->client->id ] });
    ok !$a1->closed, 'whitelisted client survives';
    ok $a2->closed, 'the other one is disconnected';
    my $a3 = client('multi');
    $hub->api('disconnect', { user => 'multi', client => $a3->client->id });
    ok $a3->closed && !$a1->closed, 'disconnect a single client by id';
    $a1->client->closed;
}

# ---- two nodes sharing one engine (cluster) ----------------------------------------
{
    my ($n1, $s1, $e) = make_hub(node_id => 'n1');
    my ($n2) = make_hub(scheduler => $s1, engine => $e, node_id => 'n2');
    my $x = T::Conn->new($n1); $x->connect(token($s1, sub => 'xavier'));
    my $y = T::Conn->new($n2); $y->connect(token($s1, sub => 'yvonne'));
    $x->cmd({ subscribe => { channel => 'room' } }); $x->take;
    $y->cmd({ subscribe => { channel => 'room' } });
    $y->take;
    my ($j) = pushes($x);
    is $j->{push}{join}{info}{user}, 'yvonne', 'join event crosses nodes';

    $y->cmd({ publish => { channel => 'room', data => 'from node 2' } }); $y->take;
    my ($m) = pushes($x);
    is $m->{push}{pub}{data}, 'from node 2', 'publication crosses nodes';
    is $m->{push}{pub}{offset}, 1, 'with a consistent offset';
    is scalar(keys %{ $n1->presence('room') }), 2, 'both nodes see the same presence';

    $n1->api('publish', { channel => 'room', data => 'from api on node 1' });
    my @got = map { $_->{push}{pub}{offset} } pushes($y);
    is_deeply \@got, [ 2 ], 'api publish on node 1 reaches node 2 clients';

    $n1->api('disconnect', { user => 'yvonne', disconnect => { code => 3000, reason => 'x' } });
    ok $y->closed, 'disconnect by user works across nodes';
    ok !$x->closed, 'and only hits that user';

    $x->cmd({ subscribe => { channel => 'other' } }); $x->take;
    $n2->api('unsubscribe', { user => 'xavier', channel => 'other' });
    is_deeply [ $x->client->subscriptions ], [ 'room' ], 'unsubscribe by user works across nodes';
    $x->client->closed;
}

# ---- proxy hooks ----------------------------------------------------------------------
{
    my @calls;
    my %queue;
    my ($h, $sc) = make_hub(
        allow_anonymous => 0,
        namespaces => {
            default    => { allow_subscribe_for_client => 1 },
            namespaces => {
                guarded => { subscribe_proxy => 1, publish_proxy => 1, presence => 1 },
            },
        },
        hooks => {
            connect => sub { my ($req, $done) = @_; push @calls, [ connect => $req ];
                             if (($req->{data} // {})->{user}) { $queue{connect} = sub { $done->({ result => { user => $req->{data}{user}, channels => [ 'srv' ], info => { via => 'proxy' } } }) } }
                             else { $done->({ error => { code => 101, message => 'unauthorized' } }) } },
            subscribe => sub { my ($req, $done) = @_; push @calls, [ subscribe => $req ];
                               $req->{channel} eq 'guarded:no'  ? $done->({ error => { code => 103, message => 'permission denied' } })
                             : $req->{channel} eq 'guarded:kick' ? $done->({ disconnect => { code => 3507, reason => 'nope' } })
                             : $req->{channel} eq 'guarded:die'  ? die "boom\n"
                             : $req->{channel} eq 'guarded:slow' ? 1
                             : $done->({ result => { info => { role => 'admin' } } }) },
            publish   => sub { my ($req, $done) = @_; $done->({ result => { data => { wrapped => $req->{data} } } }) },
            rpc       => sub { my ($req, $done) = @_; $done->({ result => { data => { echo => $req->{data}, method => $req->{method}, user => $req->{user} } } }) },
        },
    );

    # connect via proxy, asynchronously: commands sent meanwhile are queued
    my $c = T::Conn->new($h);
    my $id = $c->cmd({ connect => { data => { user => 'pat' }, name => 'js' } });
    $c->cmd({ rpc => { method => 'hello', data => 1 } });
    is scalar($c->take), 0, 'nothing answered while the connect proxy is pending';
    is $c->client->state, 'connecting', 'state is connecting';
    $queue{connect}->();
    my @f = $c->take;
    ok $f[0]{connect}, 'connect reply first';
    is $c->client->user, 'pat', 'user from proxy';
    ok $f[0]{connect}{subs}{srv}, 'server-side subscription from proxy channels';
    is_deeply $f[1]{rpc}{data}, { echo => 1, method => 'hello', user => 'pat' }, 'queued rpc executed after connect, via proxy';
    is $calls[0][1]{name}, 'js', 'connect proxy request has the client name';
    is $calls[0][1]{transport}, 'websocket', 'and the transport';

    my $d = T::Conn->new($h);
    $d->cmd({ connect => {} });
    my ($e) = $d->take;
    is $e->{error}{code}, 101, 'proxy error reaches the client';
    ok $d->closed, 'and the connection is closed';

    # subscribe proxy
    $c->cmd({ subscribe => { channel => 'guarded:ok' } });
    ok !(($c->take)[0]{error}), 'subscribe proxy allows';
    is $c->client->{subs}{'guarded:ok'}{chan_info}{role}, 'admin', 'channel info from proxy is kept';
    $c->cmd({ subscribe => { channel => 'guarded:no' } });
    is(($c->take)[0]{error}{code}, 103, 'subscribe proxy denies');
    $c->cmd({ subscribe => { channel => 'guarded:die' } });
    is(($c->take)[0]{error}{code}, 100, 'a throwing hook becomes an internal error, not a crash');
    $c->cmd({ subscribe => { channel => 'guarded:slow' } });
    is scalar($c->take), 0, 'slow hook: pending';
    $c->cmd({ subscribe => { channel => 'guarded:slow' } });
    is(($c->take)[0]{error}{code}, 105, 'second subscribe while pending -> 105');
    $sc->advance(11);
    my ($t) = $c->take;
    is $t->{error}{code}, 100, 'hook timeout -> error';
    ok $t->{error}{temporary}, 'marked temporary';
    $c->cmd({ subscribe => { channel => 'guarded:kick' } });
    is $c->close_code, 3507, 'subscribe proxy can disconnect the client';

    # publish proxy transforms data
    my $p = T::Conn->new($h);
    $p->cmd({ connect => { data => { user => 'q' } } }); $queue{connect}->(); $p->take;
    $p->cmd({ subscribe => { channel => 'guarded:ok' } }); $p->take;
    $p->cmd({ publish => { channel => 'guarded:ok', data => 'raw' } });
    my ($pub) = pushes($p);
    is_deeply $pub->{push}{pub}{data}, { wrapped => 'raw' }, 'publish proxy rewrites the payload';

    # a client that disconnects while the hook is pending must not be resurrected
    my $g = T::Conn->new($h);
    $g->cmd({ connect => { data => { user => 'ghost' } } });
    $g->client->closed;
    $queue{connect}->();
    is scalar(grep { $_->user eq 'ghost' } values %{ $h->{clients} }), 0, 'late hook result for a closed client is ignored';
}

done_testing;
