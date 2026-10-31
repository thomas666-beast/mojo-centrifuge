use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge::Protocol;
use Mojo::JSON qw(decode_json);

my $p = Mojo::Centrifuge::Protocol->new;

subtest 'decode: connect' => sub {
    my $f = $p->decode_frame('{"id":1,"connect":{"client":"x"}}');
    is $f->{method}, 'connect';
    is $f->{id},     1;
    is $f->{params}{client}, 'x';
};

subtest 'decode: empty {} is pong' => sub {
    my $f = $p->decode_frame('{}');
    ok $f->{empty}, 'empty flag';
    is $f->{method}, 'pong';
};

subtest 'decode: subscribe / unsubscribe / presence' => sub {
    for my $m (qw(subscribe unsubscribe presence)) {
        my $f = $p->decode_frame(qq({"id":7,"$m":{"channel":"news"}}));
        is $f->{method}, $m, "$m method";
        is $f->{params}{channel}, 'news', "$m channel";
    }
};

subtest 'decode: publish' => sub {
    my $f = $p->decode_frame('{"id":9,"publish":{"channel":"c","data":{"a":1}}}');
    is $f->{method}, 'publish';
    is_deeply $f->{params}{data}, { a => 1 };
};

subtest 'decode: ping' => sub {
    my $f = $p->decode_frame('{"id":3,"ping":{}}');
    is $f->{method}, 'ping';
};

subtest 'decode: garbage' => sub {
    is $p->decode_frame('not json'), undef;
    is $p->decode_frame('[]'),       undef;
    is $p->decode_frame(''),         undef;
    is $p->decode_frame(undef),      undef;
    is $p->decode_frame('{"subscribe":"x"}'), undef, 'payload must be object';
};

subtest 'decode: unknown method' => sub {
    my $f = $p->decode_frame('{"id":1,"nope":{}}');
    is $f->{method}, 'unknown';
};

subtest 'result_frame' => sub {
    my $o = decode_json($p->result_frame(1, { channel => 'news' }));
    is $o->{id}, 1;
    is $o->{result}{channel}, 'news';
};

subtest 'result_frame without id' => sub {
    my $o = decode_json($p->result_frame(undef, {}));
    ok !exists $o->{id}, 'no id key';
};

subtest 'error_frame' => sub {
    my $o = decode_json($p->error_frame(2, 103, 'denied'));
    is $o->{id}, 2;
    is $o->{error}{code},    103;
    is $o->{error}{message}, 'denied';
};

subtest 'error_frame defaults' => sub {
    my $o = decode_json($p->error_frame(undef, undef, undef));
    is $o->{error}{code},    100;
    is $o->{error}{message}, 'internal error';
};

subtest 'push_frame pub' => sub {
    my $o = decode_json($p->push_frame(channel => 'news', seq => 5, data => { x => 1 }));
    is $o->{push}{channel}, 'news';
    is $o->{push}{pub}{offset}, 5;
    is_deeply $o->{push}{pub}{data}, { x => 1 };
};

subtest 'push_frame join' => sub {
    my $o = decode_json($p->push_frame(channel => 'news', join => { client => 'c1', user => 'u' }));
    is $o->{push}{join}{client}, 'c1';
    is $o->{push}{join}{user},   'u';
};

subtest 'push_frame leave' => sub {
    my $o = decode_json($p->push_frame(channel => 'news', leave => { client => 'c1', user => 'u' }));
    is $o->{push}{leave}{client}, 'c1';
};

subtest 'ping_frame / pong_frame' => sub {
    is $p->ping_frame, '{}';
    is $p->pong_frame, '{}';
};

done_testing;
