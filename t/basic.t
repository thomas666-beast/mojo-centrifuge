use strict;
use warnings;
use Test::More;
use lib 'lib', 't/lib';
use Test::Mojo;
use Mojolicious;
use Mojo::Redis;
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::ExampleApp;

plan skip_all => 'set CENTRIFUGE_TEST_REDIS=redis://127.0.0.1:6379/0'
    unless $ENV{CENTRIFUGE_TEST_REDIS};

my $API_KEY = 'test-api-key';
my $redis = Mojo::Redis->new($ENV{CENTRIFUGE_TEST_REDIS});
my $auth  = Mojo::Centrifuge::Auth->new(secret => 's');
my $hub   = Mojo::Centrifuge->new(
    redis              => $redis,
    auth               => $auth,
    protocol           => 'centrifuge',
    publish_rate_limit => 50,
    message_rate_limit => 200,
);

my $app = Mojolicious->new;
Mojo::Centrifuge::ExampleApp::setup(hub => $hub, app => $app, api_key => $API_KEY);

my $t = Test::Mojo->new($app);

# ────────── subtests ──────────

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

subtest 'info reflects publish + subscribe' => sub {
    $t->post_ok('/api/history_remove' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'info:c' })->status_is(200);

    $t->post_ok('/api/publish' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'info:c', data => 'x' })->status_is(200);

    $t->post_ok('/api/info' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'info:c' })
      ->status_is(200)
      ->json_like('/last_offset', qr/^\d+$/)
      ->json_is('/history_length', 1)
      ->json_has('/epoch')
      ->json_is('/num_clients', 0);
};

subtest 'history_remove clears the channel' => sub {
    $t->post_ok('/api/history_remove' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'rm:c' })->status_is(200);

    $t->post_ok('/api/publish' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'rm:c', data => 'a' })->status_is(200);
    $t->post_ok('/api/publish' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'rm:c', data => 'b' })->status_is(200);

    $t->post_ok('/api/history_remove' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'rm:c' })
      ->status_is(200)->json_is('/removed', 2);

    $t->post_ok('/api/history' => { 'X-API-Key' => $API_KEY }
               => json => { channel => 'rm:c' })
      ->status_is(200)->json_is('/publications', []);
};

subtest 'revoke requires user' => sub {
    $t->post_ok('/api/revoke' => { 'X-API-Key' => $API_KEY }
               => json => {})->status_is(400);
};

subtest 'metrics renders prometheus' => sub {
    $t->get_ok('/metrics')
      ->status_is(200)
      ->content_like(qr/^centrifuge_sessions \d+$/m)
      ->content_like(qr/^centrifuge_connections \d+$/m);
};

done_testing;
