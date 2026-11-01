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
my $hub = Mojo::Centrifuge->new(
    log                  => app->log,
    redis                => $redis,
    prefix               => 'centrifuge',
    auth                 => $auth,
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

sub _require_api {
    my $c = shift;
    unless (_is_api_caller($c)) {
        $c->render(status => 401, text => "unauthorized\n");
        return 0;
    }
    return 1;
}

sub _body_json {
    my $c = shift;
    my $b = $c->req->json;
    return ref $b eq 'HASH' ? $b : {};
}

post '/publish' => sub {
    my $c = shift;

    my $body = $c->req->json;
    return $c->render(status => 400, text => "json body required\n")
        unless ref $body eq 'HASH';

    my $channel = $body->{channel};
    return $c->render(status => 400, text => "bad channel\n")
        unless defined $channel && !ref $channel && length $channel;

    unless (_is_api_caller($c)) {
        my $identity = $hub->authenticate($c->param('token') // _bearer($c));
        return $c->render(status => 401, text => "unauthorized\n") unless $identity;
        unless ($hub->authorize_publish($identity, $channel)) {
            return $c->render(status => 403,
                text => "forbidden: token not allowed to publish to '$channel'\n");
        }
    }

    my $tags = ref $body->{tags} eq 'HASH' ? $body->{tags} : undef;
    my $seq  = $hub->publish($channel, $body->{data}, $tags);
    $c->render(json => { seq => $seq, channel => $channel,
        ($tags ? (tags => $tags) : ()) });
};

post '/api/publish' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $ch = $b->{channel};
    return $c->render(status => 400, json => { error => 'channel required' })
        unless defined $ch && !ref $ch && length $ch;

    my $tags = ref $b->{tags} eq 'HASH' ? $b->{tags} : undef;
    my $seq = $hub->publish($ch, $b->{data}, $tags);
    $c->render(json => { seq => $seq, channel => $ch, ($tags ? (tags => $tags) : ()) });
};

post '/api/broadcast' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $channels = $b->{channels};
    return $c->render(status => 400, json => { error => 'channels array required' })
        unless ref $channels eq 'ARRAY' && @$channels;

    my @results;
    for my $ch (@$channels) {
        next unless defined $ch && !ref $ch && length $ch;
        my $tags = ref $b->{tags} eq 'HASH' ? $b->{tags} : undef;
        push @results, { channel => $ch, seq => $hub->publish($ch, $b->{data}, $tags) };
    }
    $c->render(json => { results => \@results });
};

post '/api/history' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $ch = $b->{channel} // 'default';
    my $since = $b->{since};
    my $limit = $b->{limit};

    my $pub = $hub->history_for($ch, $since);
    if (defined $limit && $limit =~ /^\d+$/ && $limit > 0) {
        $pub = [ @$pub[ -$limit .. -1 ] ] if @$pub > $limit;
    }
    $c->render(json => { channel => $ch, publications => $pub });
};

post '/api/presence' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $ch = $b->{channel} // 'default';
    $c->render(json => { channel => $ch, presence => $hub->presence($ch) });
};

post '/api/channels' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $stats = $hub->stats;
    my @out;
    for my $ch (sort keys %{ $stats->{channels} }) {
        push @out, {
            channel     => $ch,
            num_clients => $stats->{channels}{$ch}{connections},
        };
    }
    $c->render(json => { channels => \@out });
};

post '/api/unsubscribe' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $ch = $b->{channel};
    return $c->render(status => 400, json => { error => 'channel required' })
        unless defined $ch && length $ch;

    my $target = $b->{client};   # optional: only this client id
    my $subs = $hub->channels->{$ch} // {};
    my @kicked;
    for my $id (keys %$subs) {
        next if defined $target && $id ne $target;
        $hub->unsubscribe($id, $ch);
        push @kicked, $id;
    }
    $c->render(json => { channel => $ch, unsubscribed => \@kicked });
};

post '/api/disconnect' => sub {
    my $c = shift;
    return unless _require_api($c);

    my $b = _body_json($c);
    my $client = $b->{client};
    my $user   = $b->{user};

    my @kicked;
    for my $id (keys %{ $hub->connections }) {
        my $s = $hub->connections->{$id};
        next if defined $client && $id ne $client;
        next if defined $user   && ($s->{user} // '') ne $user;
        next unless defined $client || defined $user;
        my $tx = $s->{tx};
        eval { $tx->finish } if $tx && !$tx->is_finished;
        $hub->drop_session($id);
        push @kicked, $id;
    }
    $c->render(json => { disconnected => \@kicked });
};

get '/api/status' => sub {
    my $c = shift;
    $c->render(json => $hub->stats);
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
