use Mojo::Base -strict;
use lib 'lib';
use Mojolicious::Lite;
use Mojo::Redis;
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Mojo::Centrifuge::ExampleApp;

my $SECRET  = $ENV{CENTRIFUGE_SECRET}  // 'dev-secret-change-me';
my $API_KEY = $ENV{CENTRIFUGE_API_KEY} // 'dev-api-key-change-me';
app->log->warn('CENTRIFUGE_API_KEY not set; using the insecure dev default')
    unless $ENV{CENTRIFUGE_API_KEY};

my $redis = Mojo::Redis->new('redis://127.0.0.1:6379/0');
my $auth  = Mojo::Centrifuge::Auth->new(secret => $SECRET);
my $hub   = Mojo::Centrifuge->new(
    log                  => app->log,
    redis                => $redis,
    prefix               => 'centrifuge',
    auth                 => $auth,
    publish_rate_limit   => $ENV{CENTRIFUGE_PUBLISH_RATE} // 50,
    message_rate_limit   => $ENV{CENTRIFUGE_MESSAGE_RATE} // 200,
    client_channel_limit => 128,
    namespaces           => {
        chat => {
            history_size    => 50,
            history_ttl     => 3600,
            presence        => 1,
            presence_ttl    => 60,
            presence_events => 1,
        },
        notifications => {
            history_size    => 20,
            history_ttl     => 300,
            presence        => 0,
            presence_events => 0,
        },
    },
);

get '/token' => sub { ... unchanged ... };

websocket '/ws' => sub { ... unchanged ... };

Mojo::Centrifuge::ExampleApp::setup(hub => $hub, app => app, api_key => $API_KEY);

get '/history' => sub {
    my $c = shift;
    $c->render(json => $hub->history_for(
        $c->param('channel') // 'default',
        $c->param('since'),
    ));
};

get '/presence' => sub {
    my $c = shift;
    $c->render(json => $hub->presence($c->param('channel') // 'default'));
};

app->start;

__END__

=head1 NAME

example.pl - Minimal Mojolicious app demonstrating Mojo::Centrifuge

=head1 SYNOPSIS

  CENTRIFUGE_SECRET=s3cr3t CENTRIFUGE_API_KEY=dev \
      perl -Ilib example.pl daemon -l http://*:3000

=head1 DESCRIPTION

Boots a Mojolicious::Lite app with the following endpoints:

  GET  /token          issue a client token
  WS   /ws             WebSocket (pre-upgrade auth, real 401)
  POST /publish        backend or client publish (JSON body)
  GET  /history        read-only history shim (demo only)
  GET  /presence       read-only presence shim (demo only)

  POST /api/publish    backend publish        (X-API-Key / Bearer <api_key>)
  POST /api/broadcast  publish to many channels
  POST /api/history    read history           (with optional since/limit)
  POST /api/presence   read presence
  POST /api/channels   list channels + client counts
  POST /api/unsubscribe remove sessions from a channel
  POST /api/disconnect  close sessions (by client id or user)
  GET  /api/status     runtime counters

This script is an example only; it is B<not> installed by
C<Makefile.PL>.

=cut
