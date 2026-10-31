use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge::Auth;

my $auth = Mojo::Centrifuge::Auth->new(secret => 's3cr3t');

subtest 'verify still succeeds with secure_compare' => sub {
    my $tok = $auth->make_token(user => 'alice', ttl => 60);
    my $p   = $auth->verify_token($tok);
    is ref $p, 'HASH';
    is $p->{user}, 'alice';
};

subtest 'verify rejects wrong secret' => sub {
    my $other = Mojo::Centrifuge::Auth->new(secret => 'different');
    my $tok   = $auth->make_token(user => 'alice');
    is $other->verify_token($tok), undef;
};

subtest 'verify rejects tampered signature' => sub {
    my $tok = $auth->make_token(user => 'alice');
    my ($enc, $sig) = split /\./, $tok, 2;
    my $bad = ($sig =~ /^A/) ? 'B' . substr($sig, 1) : 'A' . substr($sig, 1);
    is $auth->verify_token("$enc.$bad"), undef;
};

subtest 'verify rejects wrong-length signature (no eq short-circuit)' => sub {
    my $tok = $auth->make_token(user => 'alice');
    my ($enc) = split /\./, $tok, 2;
    is $auth->verify_token("$enc.x"),          undef, '1-char sig';
    is $auth->verify_token("$enc." . ('A' x 100)), undef, 'long sig';
};

subtest 'expired token rejected' => sub {
    my $tok = $auth->make_token(user => 'old', ttl => -1);
    is $auth->verify_token($tok), undef;
};

done_testing;
