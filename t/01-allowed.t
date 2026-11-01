use strict;
use warnings;
use Test::More;
use lib 'lib';
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;

my $auth = Mojo::Centrifuge::Auth->new(secret => 's');
my $hub  = Mojo::Centrifuge->new(redis => undef, auth => $auth);

sub ident {
    my (%raw) = @_;
    return { user => 'u', raw => \%raw };
}

subtest '_match globs' => sub {
    ok   $hub->_match('news',   'news'),       'exact';
    ok   $hub->_match('news/*', 'news/tech'),  'trailing *';
    ok   $hub->_match('*',      'anything'),   'bare *';
    ok   $hub->_match('a?c',    'abc'),        '?';
    ok ! $hub->_match('news',   'news/tech'),  'no over-match';
    ok ! $hub->_match('a?c',    'ac'),         '? requires one char';
    ok   $hub->_match(undef,    'anything'),   'undef pattern matches all';
};

subtest '_allowed fallback chain' => sub {
    my $i_pub  = ident(publish => ['pub/*']);
    my $i_sub  = ident(subscribe => ['sub/*']);
    my $i_chan = ident(channels  => ['chan/*']);
    my $i_leg  = ident(channel   => 'legacy');
    my $i_none = ident();

    ok   $hub->_allowed($i_pub,  'pub/x',    'publish'),   'publish wins';
    ok ! $hub->_allowed($i_pub,  'pub/x',    'subscribe'), 'publish not used for subscribe';
    ok   $hub->_allowed($i_sub,  'sub/x',    'subscribe'), 'subscribe';
    ok   $hub->_allowed($i_chan, 'chan/x',   'publish'),   'channels fallback for publish';
    ok   $hub->_allowed($i_chan, 'chan/x',   'subscribe'), 'channels fallback for subscribe';
    ok   $hub->_allowed($i_leg,  'legacy',   'publish'),   'legacy channel';
    ok ! $hub->_allowed($i_leg,  'other',    'publish'),   'legacy channel denies other';
    ok ! $hub->_allowed($i_none, 'anything', 'publish'),   'deny by default';
};

subtest '_allowed from real token' => sub {
    my $tok = $auth->make_token(
        user      => 'dave',
        subscribe => ['room/*'],
        publish   => ['room/main'],
    );
    my $i = $hub->authenticate($tok);
    ok   $hub->_allowed($i, 'room/main', 'publish'),   'publish allowed';
    ok ! $hub->_allowed($i, 'room/other','publish'),   'publish denied';
    ok   $hub->_allowed($i, 'room/other','subscribe'), 'subscribe allowed';
    ok ! $hub->_allowed($i, 'other',     'subscribe'), 'subscribe denied';
};

subtest 'authenticate without auth configured is anonymous' => sub {
    my $bare = Mojo::Centrifuge->new(redis => undef);
    my $i = $bare->authenticate('whatever');
    is $i->{user}, 'anonymous', 'anonymous';
    is $i->{bound_channel}, undef, 'no bound channel';
};

subtest 'server channels (#) denied for clients' => sub {
    my $tok = $auth->make_token(user => 'alice', subscribe => ['*'], publish => ['*']);
    my $i   = $hub->authenticate($tok);
    ok !$hub->_allowed($i, '#internal', 'subscribe'), 'subscribe denied';
    ok !$hub->_allowed($i, '#internal', 'publish'),   'publish denied';
};

subtest 'user channels ($) restricted to matching user' => sub {
    my $alice = $hub->authenticate($auth->make_token(user => 'alice', subscribe => ['*']));
    my $bob   = $hub->authenticate($auth->make_token(user => 'bob',   subscribe => ['*']));

    ok  $hub->_allowed($alice, '$alice', 'subscribe'), 'alice → $alice';
    ok !$hub->_allowed($alice, '$bob',   'subscribe'), 'alice ✗ $bob';
    ok  $hub->_allowed($bob,   '$bob',   'subscribe'), 'bob → $bob';
    ok !$hub->_allowed($bob,   '$alice', 'subscribe'), 'bob ✗ $alice';
};

subtest 'user channel still requires subscribe pattern' => sub {
    my $i = $hub->authenticate($auth->make_token(user => 'alice'));  # no patterns
    ok !$hub->_allowed($i, '$alice', 'subscribe'), 'no pattern → denied';
};

done_testing;
