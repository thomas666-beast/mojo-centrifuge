package Mojo::Centrifuge::Util;
use strict;
use warnings;
use JSON::PP ();
use Exporter 'import';

our @EXPORT_OK = qw(encode_json decode_json uuid TRUE FALSE bus_name pub_payload str);
use constant CONTROL => 'ctl';

# All internal strings are *character* strings (no ->utf8). Transports/engines
# encode to bytes at their own boundary.
my $JSON = JSON::PP->new->canonical->allow_nonref;

sub encode_json { return $JSON->encode($_[0]) }
sub decode_json { return $JSON->decode($_[0]) }

sub TRUE  () { JSON::PP::true }
sub FALSE () { JSON::PP::false }

# Force a Perl scalar to be encoded as a JSON string (never as a number).
sub str { return defined $_[0] ? "$_[0]" : undef }

sub bus_name { return 'ch:' . $_[0] }

# Same layout the Redis Lua script builds, so every engine emits identical bus
# messages: {t:"pub", ch, o(ffset), e(poch), d(ata), i(nfo)}
sub pub_payload {
    my ($ch, $offset, $epoch, $data_json, $info_json) = @_;
    return '{"t":"pub","ch":' . $JSON->encode(str($ch))
        . ',"o":' . int($offset // 0)
        . ',"e":' . $JSON->encode(str($epoch // ''))
        . ',"d":' . ($data_json // 'null')
        . ',"i":' . ($info_json // 'null') . '}';
}

sub uuid {
    my $b = '';
    if (open my $fh, '<:raw', '/dev/urandom') { read $fh, $b, 16; close $fh }
    $b = pack('N4', map { int rand 4_294_967_296 } 1 .. 4) unless length $b == 16;
    my @o = unpack 'C16', $b;
    $o[6] = ($o[6] & 0x0f) | 0x40;
    $o[8] = ($o[8] & 0x3f) | 0x80;
    my $h = join '', map { sprintf '%02x', $_ } @o;
    return join '-', substr($h, 0, 8), substr($h, 8, 4), substr($h, 12, 4), substr($h, 16, 4), substr($h, 20);
}

1;
