package Mojo::Centrifuge::Auth;
use Mojo::Base -base;
use Mojo::JSON qw(encode_json decode_json);
use Mojo::Util qw(secure_compare);
use Digest::SHA qw(hmac_sha256_base64);
use MIME::Base64 qw(encode_base64 decode_base64);

our $VERSION = '0.30';

has 'secret';

sub _sign {
    my ($self, $data) = @_;
    my $sig = hmac_sha256_base64($data, $self->secret);
    $sig =~ tr{+/}{-_};
    $sig =~ s/=+$//;
    return $sig;
}

sub _b64_encode {
    my $s = encode_base64($_[0], '');
    $s =~ tr{+/}{-_};
    $s =~ s/=+$//;
    return $s;
}

sub _b64_decode {
    my $s = $_[0];
    $s =~ tr{-_}{+/};
    $s .= '=' x ((4 - length($s) % 4) % 4);
    return decode_base64($s);
}

sub make_token {
    my ($self, %args) = @_;

    my $now = time;
    my $payload = {
        user => $args{user} // 'anonymous',
        iat  => $now,
        exp  => $args{ttl} ? $now + $args{ttl} : $now + 3600,
    };

    $payload->{channel} = $args{channel} if defined $args{channel};

    $payload->{channels}  = $args{channels}  if $args{channels}  && ref $args{channels}  eq 'ARRAY';
    $payload->{subscribe} = $args{subscribe} if $args{subscribe} && ref $args{subscribe} eq 'ARRAY';
    $payload->{publish}   = $args{publish}   if $args{publish}   && ref $args{publish}   eq 'ARRAY';

    my $json = encode_json($payload);
    my $enc  = _b64_encode($json);
    my $sig  = $self->_sign($enc);

    return "$enc.$sig";
}

sub verify_token {
    my ($self, $token) = @_;

    return undef unless defined $token && length $token;

    my ($enc, $sig) = split /\./, $token, 2;
    return undef unless defined $enc && defined $sig;

    my $expected = $self->_sign($enc);
    return undef unless secure_compare($sig, $expected);

    my $json = eval { _b64_decode($enc) };
    return undef unless defined $json;

    my $payload = eval { decode_json($json) };
    return undef unless ref $payload eq 'HASH';

    return undef if $payload->{exp} && $payload->{exp} < time;

    return $payload;
}

1;

__END__

=head1 NAME

Mojo::Centrifuge::Auth - HMAC token creation and verification for Mojo::Centrifuge

=head1 SYNOPSIS

  use Mojo::Centrifuge::Auth;

  my $auth = Mojo::Centrifuge::Auth->new(secret => 's3cr3t');

  my $token = $auth->make_token(
      user      => 'alice',
      channel   => 'news',
      channels  => ['room/*'],
      subscribe => ['room/main'],
      publish   => ['room/chat'],
      ttl       => 3600,
  );

  my $payload = $auth->verify_token($token)
      or die "bad or expired token";

=head1 DESCRIPTION

Creates and verifies opaque HMAC-SHA256 tokens. The wire format is
C<< <base64url(json payload)> . <base64url(signature)> >>. The JSON
payload always carries C<user>, C<iat>, and C<exp>; the remaining
fields are optional and used by L<Mojo::Centrifuge> for glob-based
authorization:

  channel      - legacy single channel this token is bound to
  channels     - arrayref of glob patterns allowed for both
                 subscribe and publish
  subscribe    - arrayref of glob patterns allowed for subscribe only
  publish      - arrayref of glob patterns allowed for publish only

=head1 ATTRIBUTES

L<Mojo::Centrifuge::Auth> inherits all attributes from L<Mojo::Base>
and implements the following.

=head2 secret

  my $secret = $auth->secret;
  $auth     = $auth->secret('s3cr3t');

HMAC-SHA256 secret. Must be identical on every node.

=head1 METHODS

L<Mojo::Centrifuge::Auth> inherits all methods from L<Mojo::Base>
and implements the following.

=head2 make_token

  my $token = $auth->make_token(%args);

Returns a signed token string. Recognized keys: C<user>, C<ttl>,
C<channel>, C<channels>, C<subscribe>, C<publish>. C<ttl> defaults
to 3600 seconds.

=head2 verify_token

  my $payload = $auth->verify_token($token);

Returns the decoded payload hashref if the signature is valid and
C<exp> is in the future, otherwise returns C<undef>. Compares the
signature in constant time.

=head1 SEE ALSO

L<Mojo::Centrifuge>.

=head1 LICENSE

Same as Perl itself (see the LICENSE file).

=for Pod::Coverage _sign _b64_encode _b64_decode

=cut
