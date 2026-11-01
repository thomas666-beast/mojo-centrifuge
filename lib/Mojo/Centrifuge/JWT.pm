package Mojo::Centrifuge::JWT;
use strict;
use warnings;
use JSON::PP ();
use MIME::Base64 ();
use Digest::SHA ();

# HS256 only. "alg: none" and every other algorithm are rejected on purpose.
my $JSON = JSON::PP->new->utf8->canonical->allow_nonref;

sub new {
    my ($class, %a) = @_;
    die "secret required\n" unless defined $a{secret} && length $a{secret};
    return bless {
        secret => $a{secret},
        leeway => $a{leeway} // 1,
        now    => $a{now} || sub { time },
    }, $class;
}

sub _enc {
    my $s = MIME::Base64::encode_base64($_[0], '');
    $s =~ tr{+/}{-_};
    $s =~ s/=+\z//;
    return $s;
}

sub _dec {
    my $s = shift;
    return undef unless defined $s && $s =~ /\A[A-Za-z0-9_-]*\z/ && length($s) % 4 != 1;
    $s =~ tr{-_}{+/};
    $s .= '=' x ((4 - length($s) % 4) % 4);
    return MIME::Base64::decode_base64($s);
}

sub _sign {
    my ($self, $input) = @_;
    my $key = $self->{secret};
    utf8::encode($key) if utf8::is_utf8($key);
    return _enc(Digest::SHA::hmac_sha256($input, $key));
}

sub _equal {    # constant time
    my ($x, $y) = @_;
    return 0 unless defined $x && defined $y && length $x == length $y;
    my $r = 0;
    $r |= ord(substr $x, $_, 1) ^ ord(substr $y, $_, 1) for 0 .. length($x) - 1;
    return $r == 0 ? 1 : 0;
}

sub encode {
    my ($self, $claims) = @_;
    my $h = _enc($JSON->encode({ alg => 'HS256', typ => 'JWT' }));
    my $p = _enc($JSON->encode($claims));
    return "$h.$p." . $self->_sign("$h.$p");
}

# Returns ($claims, undef) or (undef, $reason)
sub decode {
    my ($self, $token) = @_;
    return (undef, 'malformed')
        unless defined $token && !ref $token && $token =~ /\A([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\z/;
    my ($h, $p, $s) = ($1, $2, $3);

    my $header = eval { $JSON->decode(_dec($h)) };
    return (undef, 'malformed') unless ref $header eq 'HASH';
    return (undef, 'unsupported_alg') unless ($header->{alg} // '') eq 'HS256';
    return (undef, 'bad_signature') unless _equal($s, $self->_sign("$h.$p"));

    my $claims = eval { $JSON->decode(_dec($p)) };
    return (undef, 'malformed') unless ref $claims eq 'HASH';

    my $now = $self->{now}->();
    for my $k (qw(exp nbf)) {
        next unless defined $claims->{$k};
        return (undef, 'malformed') if ref $claims->{$k} || $claims->{$k} !~ /\A\d+(?:\.\d+)?\z/;
    }
    return (undef, 'expired')       if defined $claims->{exp} && $claims->{exp} + $self->{leeway} < $now;
    return (undef, 'not_yet_valid') if defined $claims->{nbf} && $claims->{nbf} - $self->{leeway} > $now;
    return ($claims, undef);
}

1;
