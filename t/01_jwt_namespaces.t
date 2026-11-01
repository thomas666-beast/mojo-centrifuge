use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Mojo::Centrifuge::JWT;
use Mojo::Centrifuge::Namespaces;
use MIME::Base64 ();

# ---- JWT ----
my $t   = 1_700_000_000;
my $jwt = Mojo::Centrifuge::JWT->new(secret => 's3cret', now => sub { $t }, leeway => 1);
my $tok = $jwt->encode({ sub => 'anna', exp => $t + 60, info => { n => 'A' } });
like $tok, qr/\A[\w-]+\.[\w-]+\.[\w-]+\z/, 'token has three base64url parts';

my ($c, $why) = $jwt->decode($tok);
is $c->{sub}, 'anna', 'valid token decodes';
is $c->{info}{n}, 'A', 'claims preserved';

($c, $why) = $jwt->decode($tok . 'x');
is $why, 'bad_signature', 'tampered signature rejected';

my ($h, $p, $s) = split /\./, $tok;
(my $p2 = $p) =~ tr/a-z/b-za/;
($c, $why) = $jwt->decode("$h.$p2.$s");
ok !$c, 'tampered payload rejected';

my $none = join '.', map { my $x = MIME::Base64::encode_base64($_, ''); $x =~ tr{+/}{-_}; $x =~ s/=+$//; $x }
    '{"alg":"none","typ":"JWT"}', '{"sub":"evil"}';
($c, $why) = $jwt->decode("$none.");
ok !$c, 'alg none rejected (malformed)';
($c, $why) = $jwt->decode("$none.AAAA");
is $why, 'unsupported_alg', 'alg none with a signature part rejected';

my $other = Mojo::Centrifuge::JWT->new(secret => 'different', now => sub { $t });
($c, $why) = $other->decode($tok);
is $why, 'bad_signature', 'wrong secret rejected';

$t += 59;
ok(($jwt->decode($tok))[0], 'still valid before exp');
$t += 3;
($c, $why) = $jwt->decode($tok);
is $why, 'expired', 'expired token rejected';

($c, $why) = $jwt->decode('garbage');
is $why, 'malformed', 'garbage rejected';
($c, $why) = $jwt->decode(undef);
is $why, 'malformed', 'undef rejected';
my $noexp = $jwt->encode({ sub => 'x' });
ok(($jwt->decode($noexp))[0], 'token without exp is accepted');
my $nbf = $jwt->encode({ sub => 'x', nbf => $t + 100 });
is(($jwt->decode($nbf))[1], 'not_yet_valid', 'nbf in the future rejected');

# ---- Namespaces ----
my $ns = Mojo::Centrifuge::Namespaces->new(
    default    => { allow_subscribe_for_client => 1 },
    namespaces => {
        chat => { history_size => 5, history_ttl => 60, force_recovery => 1, presence => 1 },
        user => { allow_user_limited_channels => 1 },
    },
);
my ($o, $name) = $ns->resolve('news');
is $name, '', 'channel without colon uses default';
ok $o->{allow_subscribe_for_client}, 'default options applied';
ok !$o->{history}, 'no history by default';

($o, $name) = $ns->resolve('chat:room1');
is $name, 'chat', 'namespace parsed';
ok $o->{recoverable} && $o->{positioned} && $o->{history}, 'recovery implies positioning and history';
ok !$o->{allow_subscribe_for_client}, 'namespaces do not inherit from default';

is(($ns->resolve('nope:x'))[1], 'unknown', 'unknown namespace');
is(($ns->resolve(':x'))[1],     'unknown', 'empty namespace name');
is(($ns->resolve(''))[1],       'bad_request', 'empty channel');
is(($ns->resolve(undef))[1],    'bad_request', 'undef channel');
is(($ns->resolve({}))[1],       'bad_request', 'ref channel');
is(($ns->resolve('x' x 256))[1], 'bad_request', 'channel too long');
ok(($ns->resolve('x' x 255))[0], 'channel at the limit is fine');

is $ns->user_limited('news', 'anna'), undef, 'plain channel is not user-limited';
is $ns->user_limited('user:inbox#anna,bob', 'bob'), 1, 'listed user allowed';
is $ns->user_limited('user:inbox#anna,bob', 'eve'), 0, 'other user rejected';
is $ns->user_limited('user:inbox#anna', ''), 0, 'anonymous never matches';

# ---- config validation ----
for my $bad (
    [ { default => { historysize => 1 } },                             qr/unknown option/ ],
    [ { default => { history_size => 5 } },                            qr/set together/ ],
    [ { default => { force_recovery => 1 } },                          qr/need history/ ],
    [ { default => { history_size => 'x', history_ttl => 1 } },        qr/non-negative integer/ ],
    [ { namespaces => { 'a:b' => {} } },                               qr/invalid namespace name/ ],
    [ { namespaces => { x => [] } },                                   qr/must be a hash/ ],
) {
    eval { Mojo::Centrifuge::Namespaces->new(%{ $bad->[0] }) };
    like $@, $bad->[1], "bad config rejected: $bad->[1]";
}

done_testing;
