use strict;
use warnings;
use Test::More;
use File::Spec;
use File::Basename qw(dirname);
use Cwd qw(abs_path);

my $t_dir  = dirname(abs_path(__FILE__));
my $root   = abs_path(File::Spec->catdir($t_dir, File::Spec->updir));
my $cli    = File::Spec->catfile($root, 'make_token.pl');
my $perl   = $^X;

sub run_cli {
    my @args = @_;
    my $cmd  = join ' ', map { "'" . ($_ =~ s/'/'\\''/gr) . "'" } $perl, $cli, @args;
    my $out  = `$cmd 2>&1`;
    return ($? >> 8, $out);
}

subtest 'positional args' => sub {
    my ($code, $out) = run_cli('alice', 'news', '60');
    is $code, 0, 'exit 0';
    like $out, qr/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n$/, 'token format';
};

subtest 'positional args, no channel' => sub {
    my ($code, $out) = run_cli('alice');
    is $code, 0, 'exit 0';
    like $out, qr/\./, 'has signature separator';
};

subtest '--subscribe / --publish' => sub {
    my ($code, $out) = run_cli('--user=alice', '--subscribe=room/*', '--publish=room/main');
    is $code, 0, 'exit 0';
    like $out, qr/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n$/, 'token format';
};

subtest '--channels repeatable' => sub {
    my ($code, $out) = run_cli('--user=bob', '--channels=a/*', '--channels=b/*');
    is $code, 0, 'exit 0';
    like $out, qr/\./, 'has signature separator';
};

subtest 'bad option exits non-zero' => sub {
    my ($code, $out) = run_cli('--bogus');
    isnt $code, 0, 'non-zero exit';
};

done_testing;
