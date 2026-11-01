use Mojo::Base -strict;
use lib 'lib';
use Mojolicious::Lite;
use Mojo::Redis;
use Mojo::Util qw(secure_compare encode);
use Mojo::Centrifuge;
use Mojo::Centrifuge::Engine::Redis;
use Mojo::Centrifuge::Util qw(encode_json);

# ---- configuration ----------------------------------------------------------
my $SECRET  = $ENV{CENTRIFUGE_SECRET}  // 'dev-secret-change-me';      # signs JWTs (HS256)
my $API_KEY = $ENV{CENTRIFUGE_API_KEY} // 'dev-api-key-change-me';      # for /api
my $REDIS   = $ENV{REDIS_URL}          // 'redis://127.0.0.1:6379/0';

app->log->warn('CENTRIFUGE_SECRET not set: using the insecure dev default')  unless $ENV{CENTRIFUGE_SECRET};
app->log->warn('CENTRIFUGE_API_KEY not set: using the insecure dev default') unless $ENV{CENTRIFUGE_API_KEY};

my $hub = Mojo::Centrifuge->new(
    engine  => Mojo::Centrifuge::Engine::Redis->new(redis => Mojo::Redis->new($REDIS), prefix => 'centrifuge'),
    secret  => $SECRET,
    api_key => $API_KEY,
    log     => app->log,

    # Channel "chat:room1" lives in namespace "chat"; "news" uses `default`.
    namespaces => {
        default => {
            history_size => 100, history_ttl => 300, force_recovery => 1,
            presence => 1, join_leave => 1,
            allow_subscribe_for_client => 1, allow_publish_for_client => 1,
            allow_presence_for_client  => 1, allow_history_for_client => 1,
        },
        namespaces => {
            chat => {
                history_size => 50, history_ttl => 3600, force_recovery => 1,
                presence => 1, join_leave => 1,
                allow_subscribe_for_client   => 1, allow_publish_for_subscriber  => 1,
                allow_presence_for_subscriber => 1, allow_history_for_subscriber => 1,
            },
            # "inbox:#anna" or "inbox:#anna,bob": only the listed users may subscribe
            inbox => {
                history_size => 20, history_ttl => 600, force_recovery => 1,
                allow_user_limited_channels => 1, allow_history_for_subscriber => 1,
            },
            # needs a subscription token (see /sub-token)
            private => {
                presence => 1, allow_publish_for_subscriber => 1, allow_presence_for_subscriber => 1,
            },
        },
    },

    # Optional: ask your backend instead of (or besides) JWTs.
    # hooks => { connect => Mojo::Centrifuge::Proxy::HTTP->new(endpoint => 'http://127.0.0.1:9000/connect')->hook },
);

# ---- clients: Centrifugo JSON protocol over WebSocket --------------------------
websocket '/connection/websocket' => sub {
    my $c = shift;
    $c->inactivity_timeout(120);    # the server pings every 25s

    my $client = $hub->new_client(
        send  => sub { $c->send($_[0]) },
        close => sub { my ($code, $reason) = @_; $c->finish($code, $reason) },
    );
    $c->on(message => sub { my ($c, $msg) = @_; $client->handle_frame($msg) });
    $c->on(finish  => sub { $client->closed });
};

# ---- server API (same shape as Centrifugo's HTTP API) ---------------------------
sub api_authorized {
    my $c = shift;
    my $h = $c->req->headers->authorization // '';
    my $key = $h =~ /\Aapikey\s+(\S+)\s*\z/i ? $1 : $c->req->headers->header('X-API-Key');
    return defined $key && length $key && secure_compare($key, $API_KEY);
}

sub render_api {
    my ($c, $res) = @_;
    $c->res->headers->content_type('application/json; charset=utf-8');
    $c->render(data => encode('UTF-8', encode_json($res)));
}

# POST /api            {"method": "publish", "params": {...}}
post '/api' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless api_authorized($c);
    my $body = $c->req->json;
    return $c->render(status => 400, text => "bad request\n") unless ref $body eq 'HASH';
    $hub->start;
    render_api($c, $hub->api($body->{method}, $body->{params}));
};

# POST /api/publish    {...params...}
post '/api/:method' => sub {
    my $c = shift;
    return $c->render(status => 401, text => "unauthorized\n") unless api_authorized($c);
    my $body = $c->req->json // {};
    return $c->render(status => 400, text => "bad request\n") unless ref $body eq 'HASH';
    $hub->start;
    render_api($c, $hub->api($c->stash('method'), $body));
};

get '/health' => sub { shift->render(json => { status => 'ok', version => $hub->version }) };

# ---- development helpers: hand out tokens (disabled in production mode) ----------
if (app->mode eq 'development') {
    # /token?user=anna&ttl=3600&channels=inbox:%23anna   (channels = server-side subscriptions)
    get '/token' => sub {
        my $c    = shift;
        my $user = $c->param('user') // 'anonymous';
        my $ttl  = $c->param('ttl')  // 3600;
        my @chan = grep { length } @{ $c->every_param('channels') };
        my %claims = (sub => $user, exp => time + $ttl, info => { name => $user });
        $claims{channels} = \@chan if @chan;
        $c->render(text => $hub->jwt->encode(\%claims));
    };

    # /sub-token?channel=private:vip&user=anna
    get '/sub-token' => sub {
        my $c = shift;
        my $user = $c->param('user')    // 'anonymous';
        my $chan = $c->param('channel') // return $c->render(status => 400, text => "channel required\n");
        $c->render(text => $hub->jwt->encode({ sub => $user, channel => $chan, exp => time + ($c->param('ttl') // 3600) }));
    };
}

app->start;
