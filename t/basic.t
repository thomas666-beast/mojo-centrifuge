use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Test::Mojo;
use Mojolicious::Lite;
use Mojo::Redis;
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;

plan skip_all => 'set CENTRIFUGE_TEST_REDIS=redis://127.0.0.1:6379/0'
    unless $ENV{CENTRIFUGE_TEST_REDIS};

my $API_KEY = 'test-api-key';
my $redis = Mojo::Redis->new($ENV{CENTRIFUGE_TEST_REDIS});
my $auth  = Mojo::Centrifuge::Auth->new(secret => 's');
my $hub   = Mojo::Centrifuge->new(redis => $redis, auth => $auth, protocol => 'centrifuge');

# --- mount only the /api/* routes for these tests ---
app->log->level('error');

my $is_api = sub {
    my $c = shift;
    my $k = $c->req->headers->header('X-API-Key') // '';
    return $k eq $API_KEY;
};

post '/api/publish' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless $is_api->($c);
    my $b = $c->req->json;
    return $c->render(status => 400, json => { error => 'channel required' })
        unless $b && defined $b->{channel} && length $b->{channel};
    my $tags = ref $b->{tags} eq 'HASH' ? $b->{tags} : undef;
    my $seq = $hub->publish($b->{channel}, $b->{data}, $tags);
    $c->render(json => { seq => $seq, channel => $b->{channel},
        ($tags ? (tags => $tags) : ()) });
};

post '/api/broadcast' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless $is_api->($c);
    my $b = $c->req->json;
    return $c->render(status => 400, json => { error => 'channels required' })
        unless $b && ref $b->{channels} eq 'ARRAY';
    my @r = map { { channel => $_, seq => $hub->publish($_, $b->{data}) } }
            grep { defined && length } @{ $b->{channels} };
    $c->render(json => { results => \@r });
};

post '/api/history' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless $is_api->($c);
    my $b = $c->req->json;
    $c->render(json => { channel => $b->{channel},
        publications => $hub->history_for($b->{channel}, $b->{since}) });
};

post '/api/presence' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless $is_api->($c);
    my $b = $c->req->json;
    $c->render(json => { channel => $b->{channel},
        presence => $hub->presence($b->{channel}) });
};

my $t = Test::Mojo->new;

subtest 'publish requires API key' => sub {
    $t->post_ok('/api/publish' => json => { channel => 'x', data => 1 })
      ->status_is(401);
};

subtest 'publish with API key' => sub {
    $t->post_ok('/api/publish' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 't:1', data => { a => 1 } })
      ->status_is(200)
      ->json_has('/seq')
      ->json_is('/channel', 't:1');
};

subtest 'publish with tags' => sub {
    $t->post_ok('/api/publish' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 't:2', data => 1, tags => { src => 'test' } })
      ->status_is(200)
      ->json_is('/tags/src', 'test');
};

subtest 'broadcast' => sub {
    $t->post_ok('/api/broadcast' => { 'X-API-Key' => $API_KEY }
               => json => { channels => [ 't:3', 't:4' ], data => 'hi' })
      ->status_is(200)
      ->json_has('/results/0/seq')
      ->json_has('/results/1/seq');
};

subtest 'history' => sub {
    $t->post_ok('/api/history' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 't:1' })
      ->status_is(200)
      ->json_has('/publications');
};

subtest 'presence' => sub {
    $t->post_ok('/api/presence' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 't:1' })
      ->status_is(200)
      ->json_has('/presence');
};

done_testing;
