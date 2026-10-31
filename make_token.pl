use Mojo::Base -strict;
use lib 'lib';
use Mojo::Centrifuge::Auth;
use Getopt::Long;

my $secret = $ENV{CENTRIFUGE_SECRET} // 'dev-secret-change-me';

my ($user, $channel, $ttl);
my (@channels, @subscribe, @publish);

GetOptions(
    'user=s'       => \$user,
    'channel=s'    => \$channel,
    'channels=s@'  => \@channels,
    'subscribe=s@' => \@subscribe,
    'publish=s@'   => \@publish,
    'ttl=i'        => \$ttl,
) or die "usage: $0 [user] [channel] [ttl] [--channels X --subscribe Y --publish Z ...]\n";

$user    //= shift @ARGV // 'cli-user';
$channel //= shift @ARGV;
$ttl     //= shift @ARGV // 3600;

my $auth = Mojo::Centrifuge::Auth->new(secret => $secret);
my $token = $auth->make_token(
    user      => $user,
    channel   => $channel,
    channels  => @channels  ? \@channels  : undef,
    subscribe => @subscribe ? \@subscribe : undef,
    publish   => @publish   ? \@publish   : undef,
    ttl       => $ttl,
);
print "$token\n";

__END__

=head1 NAME

make_token.pl - CLI helper to mint Mojo::Centrifuge tokens

=head1 SYNOPSIS

  CENTRIFUGE_SECRET=s3cr3t perl make_token.pl alice news 3600

  perl make_token.pl --user=alice --subscribe='room/*' --publish='room/main'

=head1 OPTIONS

  --user       subject name (default: first positional, else 'cli-user')
  --channel    legacy single-channel binding (second positional)
  --channels   repeatable, glob patterns for both sub and pub
  --subscribe  repeatable, glob patterns for subscribe only
  --publish    repeatable, glob patterns for publish only
  --ttl        seconds (third positional, default 3600)

=head1 ENVIRONMENT

  CENTRIFUGE_SECRET   HMAC signing secret. Defaults to the dev value
                      'dev-secret-change-me' when unset. B<Do not> use
                      the default in production.

=cut
