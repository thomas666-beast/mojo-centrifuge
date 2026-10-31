package Mojo::Centrifuge::Protocol;
use Mojo::Base -base;
use Mojo::JSON qw(encode_json decode_json);
use Exporter 'import';

our $VERSION = '0.21';

our @EXPORT_OK = qw(
    ERR_INTERNAL
    ERR_UNAUTHORIZED
    ERR_PERMISSION_DENIED
    ERR_UNKNOWN_METHOD
    ERR_BAD_REQUEST
    ERR_NOT_AVAILABLE
    ERR_TOKEN_EXPIRED
    ERR_BAD_SUBSCRIBE
    ERR_BAD_PUBLISH
);

our %EXPORT_TAGS = (codes => \@EXPORT_OK);

use constant {
    ERR_INTERNAL          => 100,
    ERR_UNAUTHORIZED      => 101,
    ERR_PERMISSION_DENIED => 103,
    ERR_UNKNOWN_METHOD    => 105,
    ERR_BAD_REQUEST       => 107,
    ERR_NOT_AVAILABLE     => 108,
    ERR_TOKEN_EXPIRED     => 109,
    ERR_BAD_SUBSCRIBE     => 201,
    ERR_BAD_PUBLISH       => 202,
};

sub decode_frame {
    my ($self, $raw) = @_;
    return undef unless defined $raw && length $raw;

    my $obj = eval { decode_json($raw) };
    return undef unless ref $obj eq 'HASH';

    return { method => 'pong', empty => 1 } unless keys %$obj;

    my $id = $obj->{id};
    if (defined $id && ref $id) { undef $id }

    for my $m (qw(connect subscribe unsubscribe publish presence refresh)) {
        next unless exists $obj->{$m};
        my $p = $obj->{$m};
        return undef unless ref $p eq 'HASH';
        return { method => $m, id => $id, params => $p };
    }

    if (exists $obj->{ping}) {
        return { method => 'ping', id => $id, params => {} };
    }

    return { method => 'unknown', id => $id, raw => $obj };
}

sub result_frame {
    my ($self, $id, $result) = @_;
    my %f;
    $f{id}     = $id     if defined $id;
    $f{result} = $result // {};
    return encode_json(\%f);
}

sub error_frame {
    my ($self, $id, $code, $message) = @_;
    my %f;
    $f{id}    = $id if defined $id;
    $f{error} = {
        code    => defined $code    ? $code    : ERR_INTERNAL,
        message => defined $message ? $message : 'internal error',
    };
    return encode_json(\%f);
}

sub push_frame {
    my ($self, %args) = @_;
    my $channel = $args{channel};
    my $push    = { channel => $channel };

    if (exists $args{join} || exists $args{leave}) {
        my $kind = exists $args{join} ? 'join' : 'leave';
        $push->{$kind} = $args{$kind};
    }
    elsif (exists $args{data} || exists $args{seq}) {
        $push->{pub} = {
            data   => $args{data},
            offset => defined $args{seq} ? $args{seq} : 0,
        };
    }
    return encode_json({ push => $push });
}

sub ping_frame { return '{}' }
sub pong_frame { return '{}' }

1;

__END__

=head1 NAME

Mojo::Centrifuge::Protocol - Centrifugo JSON protocol frame codec

=head1 SYNOPSIS

  use Mojo::Centrifuge::Protocol;
  use Mojo::Centrifuge::Protocol qw(:codes);

  my $p = Mojo::Centrifuge::Protocol->new;

  my $f = $p->decode_frame('{"id":1,"subscribe":{"channel":"news"}}');
  # { method => 'subscribe', id => 1, params => { channel => 'news' } }

  my $ok  = $p->result_frame(1, { channel => 'news' });
  my $err = $p->error_frame(1, ERR_PERMISSION_DENIED, 'not authorized');
  my $pu  = $p->push_frame(channel => 'news', seq => 5, data => { x => 1 });

=head1 DESCRIPTION

Encodes and decodes the JSON frames of the Centrifugo protocol. The
following client frames are recognized:

  { "id": N, "connect":     { ... } }
  { "id": N, "subscribe":   { "channel": "...", "since": N? } }
  { "id": N, "unsubscribe": { "channel": "..." } }
  { "id": N, "publish":     { "channel": "...", "data": ... } }
  { "id": N, "presence":    { "channel": "..." } }
  { "id": N, "refresh":     { "token": "..." } }
  { "id": N, "ping":        {} }
  {}

An empty C<{}> is treated as a pong. Every other frame must be a JSON
object. Frames that cannot be decoded, or that carry a method payload
that is not an object, return C<undef>.

Server frames are built by C<result_frame>, C<error_frame>,
C<push_frame>, C<ping_frame>, and C<pong_frame>. The C<push> frame
carries a C<pub> (with C<data> and C<offset>) or a C<join>/C<leave>
event, mirroring Centrifugo.

=head1 EXPORTS

Nothing is exported by default. The following error codes may be
imported individually or via the C<:codes> tag:

  ERR_INTERNAL           100
  ERR_UNAUTHORIZED       101
  ERR_PERMISSION_DENIED  103
  ERR_UNKNOWN_METHOD     105
  ERR_BAD_REQUEST        107
  ERR_NOT_AVAILABLE      108
  ERR_TOKEN_EXPIRED      109
  ERR_BAD_SUBSCRIBE      201
  ERR_BAD_PUBLISH        202

=head1 METHODS

L<Mojo::Centrifuge::Protocol> inherits all methods from L<Mojo::Base>
and implements the following.

=head2 decode_frame

  my $frame = $p->decode_frame($raw_json);

Returns a hashref C<< { method, id, params } >>, or C<undef>. The
C<empty> key is set to C<1> for the C<{}> pong.

=head2 result_frame

  my $json = $p->result_frame($id, \%result);

Encodes a success reply.

=head2 error_frame

  my $json = $p->error_frame($id, $code, $message);

Encodes an error reply. C<$code> and C<$message> default to
C<ERR_INTERNAL> and C<'internal error'>.

=head2 push_frame

  my $json = $p->push_frame(%args);

Encodes a server push. Recognized keys: C<channel> (required) and
either C<data>+C<seq> (a publication) or C<join> or C<leave> (a
presence event).

=head2 ping_frame

=head2 pong_frame

Return the literal string C<'{}'>.

=head1 SEE ALSO

L<Mojo::Centrifuge>.

=head1 LICENSE

Same as Perl itself (see the LICENSE file).

=cut
