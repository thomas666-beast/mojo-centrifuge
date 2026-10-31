use Mojo::Base -strict;
use lib 'lib';
use Mojolicious::Lite;
use Mojo::Redis;
use Mojo::Centrifuge;
use Mojo::Centrifuge::Auth;
use Mojo::Util qw(secure_compare);

my $SECRET = $ENV{CENTRIFUGE_SECRET} // 'dev-secret-change-me';

my $API_KEY = $ENV{CENTRIFUGE_API_KEY} // 'dev-api-key-change-me';
app->log->warn('CENTRIFUGE_API_KEY not set; using the insecure dev default')
    unless $ENV{CENTRIFUGE_API_KEY};

my $redis = Mojo::Redis->new('redis://127.0.0.1:6379/0');
my $auth  = Mojo::Centrifuge::Auth->new(secret => $SECRET);
my $hub   = Mojo::Centrifuge->new(
    log    => app->log,
    redis  => $redis,
    prefix => 'centrifuge',
    auth   => $auth,
);

get '/token' => sub {
    my $c    = shift;
    my $user = $c->param('user') // 'anonymous';
    my $ttl  = $c->param('ttl')  // 3600;

    my @channels  = grep { defined && !ref && length } @{ $c->every_param('channels') };
    my @subscribe = grep { defined && !ref && length } @{ $c->every_param('subscribe') };
    my @publish   = grep { defined && !ref && length } @{ $c->every_param('publish') };
    my $channel   = $c->param('channel');

    my $token = $auth->make_token(
        user      => $user,
        channel   => $channel,
        channels  => @channels  ? \@channels  : undef,
        subscribe => @subscribe ? \@subscribe : undef,
        publish   => @publish   ? \@publish   : undef,
        ttl       => $ttl,
    );
    $c->render(text => "$token\n");
};

websocket '/ws' => sub {
    my $c = shift;

    my $identity = $hub->authenticate($c->param('token'));
    unless ($identity) {
        $c->app->log->warn(
            "WS auth rejected for " . ($c->tx->remote_address // '?')
        );
        return $c->render(status => 401, text => "unauthorized\n");
    }

    my $channel = $c->param('channel');
    my $since   = $c->param('since');
    eval { $hub->add_connection($c, $channel, $since, $identity); 1 }
        or $c->app->log->error("add_connection: $@");
};

sub _bearer {
    my $c = shift;
    my $h = $c->req->headers->authorization // '';
    return $h =~ /^Bearer\s+(\S+)\s*$/i ? $1 : undef;
}

sub _is_api_caller {
    my $c = shift;
    my $given = $c->req->headers->header('X-API-Key') // _bearer($c);
    return defined $given && length $given && secure_compare($given, $API_KEY);
}

any ['GET', 'POST'] => '/publish' => sub {
    my $c = shift;

    my $body = $c->req->json;
    my $json_mode = defined $body;
    $body = {} unless ref $body eq 'HASH';

    my $channel = $body->{channel} // $c->param('channel') // 'default';
    if (ref $channel || !length $channel) {
        return $c->render(status => 400, text => "bad channel\n");
    }

    unless (_is_api_caller($c)) {
        my $identity = $hub->authenticate($c->param('token') // _bearer($c));
        return $c->render(status => 401, text => "unauthorized\n") unless $identity;
        unless ($hub->authorize_publish($identity, $channel)) {
            return $c->render(
                status => 403,
                text   => "forbidden: token not allowed to publish to '$channel'\n",
            );
        }
    }

    my $data = exists $body->{data}
        ? $body->{data}
        : "Server broadcast: " . ($c->param('msg') // 'hello');

    my $seq = $hub->publish($channel, $data);
    return $json_mode
        ? $c->render(json => { seq => $seq, channel => $channel })
        : $c->render(text => "Published seq=$seq on '$channel'\n");
};

get '/history' => sub {
    my $c       = shift;
    my $channel = $c->param('channel') // 'default';
    my $since   = $c->param('since');
    $c->render(json => $hub->history_for($channel, $since));
};

get '/presence' => sub {
    my $c       = shift;
    my $channel = $c->param('channel') // 'default';
    $c->render(json => $hub->presence($channel));
};

get '/status' => sub {
    my $c = shift;
    $c->render(json => $hub->stats);
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

  GET  /token      issue a client token (user, channel, channels,
                   subscribe, publish, ttl)
  WS   /ws         WebSocket endpoint (pre-upgrade auth; real 401 on failure)
  GET|POST /publish  publish to a channel. Two callers accepted:
                     - X-API-Key / Bearer <api_key>  (backend, any channel)
                     - Bearer <token> / ?token=      (client, checked
                       against authorize_publish)
  GET  /history    list the retained history of a channel
  GET  /presence   list the current members of a channel
  GET  /status     runtime counters

This script is an example only; it is B<not> installed by
C<Makefile.PL>.

=cut
