use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge::Auth;
use Mojo::JSON qw(decode_json);

my $auth = Mojo::Centrifuge::Auth->new(secret => 's3cr3t');

subtest 'make_token produces enc.sig' => sub {
    my $tok = $auth->make_token(user => 'alice');
    like $tok, qr/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/, 'base64url.base64url';
    my ($enc, $sig) = split /\./, $tok, 2;
    is $auth->_sign($enc), $sig, 'signature matches';
};

subtest 'verify_token round-trips' => sub {
    my $tok = $auth->make_token(user => 'bob', channel => 'news', ttl => 60);
    my $p   = $auth->verify_token($tok);
    is ref $p, 'HASH', 'got payload';
    is $p->{user},    'bob',  'user preserved';
    is $p->{channel}, 'news', 'legacy channel preserved';
    ok $p->{exp} > time, 'not expired';
};

subtest 'verify_token rejects tampering' => sub {
    my $tok = $auth->make_token(user => 'eve');
    my ($enc, $sig) = split /\./, $tok, 2;

    my $flipped_sig = ($sig =~ /^A/) ? 'B' . substr($sig, 1) : 'A' . substr($sig, 1);
    is $auth->verify_token("$enc.$flipped_sig"), undef, 'bad sig rejected';

    my $flipped_enc = ($enc =~ /^A/) ? 'B' . substr($enc, 1) : 'A' . substr($enc, 1);
    is $auth->verify_token("$flipped_enc.$sig"), undef, 'tampered payload rejected';
};

subtest 'verify_token rejects malformed input' => sub {
    is $auth->verify_token(undef),   undef, 'undef';
    is $auth->verify_token(''),      undef, 'empty';
    is $auth->verify_token('nodot'), undef, 'no dot';
    is $auth->verify_token('a.b.c'), undef, 'too many parts';
    is $auth->verify_token('.sig'),  undef, 'empty enc';
};

subtest 'verify_token rejects expired' => sub {
    my $tok = $auth->make_token(user => 'old', ttl => -10);
    is $auth->verify_token($tok), undef, 'expired token rejected';
};

subtest 'multi-channel payload survives round-trip' => sub {
    my $tok = $auth->make_token(
        user      => 'carol',
        channels  => ['a/*', 'b'],
        subscribe => ['c'],
        publish   => ['d'],
    );
    my $p = $auth->verify_token($tok);
    is_deeply $p->{channels},  ['a/*', 'b'], 'channels';
    is_deeply $p->{subscribe}, ['c'],        'subscribe';
    is_deeply $p->{publish},   ['d'],        'publish';
};

subtest 'base64url helpers' => sub {
    my $enc = Mojo::Centrifuge::Auth::_b64_encode("hello\x{ff}\x00");
    unlike $enc, qr/[+\/=]/, 'no +, /, or =';
    is Mojo::Centrifuge::Auth::_b64_decode($enc), "hello\x{ff}\x00", 'round-trip';
};

done_testing;
