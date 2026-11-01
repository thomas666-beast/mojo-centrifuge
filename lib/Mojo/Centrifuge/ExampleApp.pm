package Mojo::Centrifuge::ExampleApp;
use Mojo::Base -strict;
use Mojo::Util qw(secure_compare);

our $VERSION = '0.30';

sub setup {
    my (%args) = @_;
    my $hub     = $args{hub}     or die "hub required\n";
    my $app     = $args{app}     or die "app required\n";
    my $API_KEY = $args{api_key} // 'test-api-key';
    my $r       = $app->routes;

    my $is_api = sub {
        my $c = shift;
        my $given = $c->req->headers->header('X-API-Key')
            // do {
                my $h = $c->req->headers->authorization // '';
                $h =~ /^Bearer\s+(\S+)\s*$/i ? $1 : undef;
            };
        return defined $given && length $given && secure_compare($given, $API_KEY);
    };

    my $require_api = sub {
        my $c = shift;
        unless ($is_api->($c)) {
            $c->render(status => 401, text => "unauthorized\n");
            return 0;
        }
        return 1;
    };

    my $body_json = sub {
        my $b = $_[0]->req->json;
        return ref $b eq 'HASH' ? $b : {};
    };

    $r->post('/publish' => sub {
        my $c = shift;
        my $body = $c->req->json;
        return $c->render(status => 400, text => "json body required\n")
            unless ref $body eq 'HASH';

        my $channel = $body->{channel};
        return $c->render(status => 400, text => "bad channel\n")
            unless defined $channel && !ref $channel && length $channel;

        unless ($is_api->($c)) {
            my $identity = $hub->authenticate($c->param('token'));
            return $c->render(status => 401, text => "unauthorized\n")
                unless $identity;
            unless ($hub->authorize_publish($identity, $channel)) {
                return $c->render(status => 403,
                    text => "forbidden: token not allowed to publish to '$channel'\n");
            }
        }

        my $tags = ref $body->{tags} eq 'HASH' ? $body->{tags} : undef;
        my $seq  = $hub->publish($channel, $body->{data}, $tags);
        $c->render(json => {
            seq     => $seq,
            channel => $channel,
            ($tags ? (tags => $tags) : ()),
        });
    });

    $r->post('/api/publish' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b = $body_json->($c);
        my $ch = $b->{channel};
        return $c->render(status => 400, json => { error => 'channel required' })
            unless defined $ch && !ref $ch && length $ch;

        my $tags = ref $b->{tags} eq 'HASH' ? $b->{tags} : undef;
        my $seq = $hub->publish($ch, $b->{data}, $tags);
        $c->render(json => {
            seq     => $seq,
            channel => $ch,
            ($tags ? (tags => $tags) : ()),
        });
    });

    $r->post('/api/broadcast' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b = $body_json->($c);
        my $channels = $b->{channels};
        return $c->render(status => 400, json => { error => 'channels array required' })
            unless ref $channels eq 'ARRAY' && @$channels;

        my @results;
        for my $ch (@$channels) {
            next unless defined $ch && !ref $ch && length $ch;
            my $tags = ref $b->{tags} eq 'HASH' ? $b->{tags} : undef;
            push @results, {
                channel => $ch,
                seq     => $hub->publish($ch, $b->{data}, $tags),
            };
        }
        $c->render(json => { results => \@results });
    });

    $r->post('/api/history' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b  = $body_json->($c);
        my $ch = $b->{channel} // 'default';
        my $pub = $hub->history_for($ch, $b->{since});

        if (defined $b->{limit} && $b->{limit} =~ /^\d+$/ && $b->{limit} > 0) {
            $pub = [ @$pub[ -$b->{limit} .. -1 ] ] if @$pub > $b->{limit};
        }
        $c->render(json => { channel => $ch, publications => $pub });
    });

    $r->post('/api/presence' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b  = $body_json->($c);
        my $ch = $b->{channel} // 'default';
        $c->render(json => { channel => $ch, presence => $hub->presence($ch) });
    });

    $r->post('/api/channels' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $s = $hub->stats;
        my @out;
        for my $ch (sort keys %{ $s->{channels} }) {
            push @out, {
                channel     => $ch,
                num_clients => $s->{channels}{$ch}{connections},
            };
        }
        $c->render(json => { channels => \@out });
    });

    $r->post('/api/info' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b = $body_json->($c);
        return $c->render(status => 400, json => { error => 'channel required' })
            unless defined $b->{channel} && length $b->{channel};
        $c->render(json => $hub->channel_info($b->{channel}));
    });

    $r->post('/api/history_remove' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b = $body_json->($c);
        return $c->render(status => 400, json => { error => 'channel required' })
            unless defined $b->{channel} && length $b->{channel};

        my $n = $hub->history_remove($b->{channel});
        $c->render(json => { channel => $b->{channel}, removed => $n });
    });

    $r->post('/api/revoke' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b = $body_json->($c);
        return $c->render(status => 400, json => { error => 'user required' })
            unless defined $b->{user} && length $b->{user};

        my $kicked = $hub->revoke_user($b->{user});
        $c->render(json => { user => $b->{user}, revoked => $kicked });
    });

    $r->post('/api/unsubscribe' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b  = $body_json->($c);
        my $ch = $b->{channel};
        return $c->render(status => 400, json => { error => 'channel required' })
            unless defined $ch && length $ch;

        my $target = $b->{client};
        my $subs   = $hub->channels->{$ch} // {};
        my @kicked;
        for my $id (keys %$subs) {
            next if defined $target && $id ne $target;
            $hub->unsubscribe($id, $ch);
            push @kicked, $id;
        }
        $c->render(json => { channel => $ch, unsubscribed => \@kicked });
    });

    $r->post('/api/disconnect' => sub {
        my $c = shift;
        return unless $require_api->($c);

        my $b      = $body_json->($c);
        my $client = $b->{client};
        my $user   = $b->{user};
        return $c->render(status => 400, json => { error => 'client or user required' })
            unless defined $client || defined $user;

        my @kicked;
        for my $id (keys %{ $hub->connections }) {
            my $s = $hub->connections->{$id};
            next if defined $client && $id ne $client;
            next if defined $user   && ($s->{user} // '') ne $user;
            my $tx = $s->{tx};
            eval { $tx->finish } if $tx && !$tx->is_finished;
            $hub->drop_session($id);
            push @kicked, $id;
        }
        $c->render(json => { disconnected => \@kicked });
    });

    $r->get('/api/status' => sub {
        my $c = shift;
        $c->render(json => $hub->stats);
    });

    $r->get('/metrics' => sub {
        my $c = shift;
        my $s = $hub->stats;

        my @lines;
        push @lines, "# HELP centrifuge_connections Total active subscriptions on this node";
        push @lines, "# TYPE centrifuge_connections gauge";
        push @lines, "centrifuge_connections $s->{connections}";

        push @lines, "# HELP centrifuge_sessions Total WebSocket sessions on this node";
        push @lines, "# TYPE centrifuge_sessions gauge";
        push @lines, "centrifuge_sessions $s->{sessions}";

        push @lines, "# HELP centrifuge_channel_connections Subscribers per channel";
        push @lines, "# TYPE centrifuge_channel_connections gauge";
        for my $ch (sort keys %{ $s->{channels} }) {
            my $n  = $s->{channels}{$ch}{connections};
            my $ns = $s->{channels}{$ch}{namespace} // '';
            push @lines, qq{centrifuge_channel_connections{channel="$ch",namespace="$ns"} $n};
        }

        push @lines, "# HELP centrifuge_channel_history Retained history entries per channel";
        push @lines, "# TYPE centrifuge_channel_history gauge";
        for my $ch (sort keys %{ $s->{channels} }) {
            my $n = $s->{channels}{$ch}{history_len};
            push @lines, qq{centrifuge_channel_history{channel="$ch"} $n};
        }

        push @lines, "# HELP centrifuge_channel_last_offset Last sequence per channel";
        push @lines, "# TYPE centrifuge_channel_last_offset gauge";
        for my $ch (sort keys %{ $s->{channels} }) {
            my $n = $s->{channels}{$ch}{last_seq} // 0;
            push @lines, qq{centrifuge_channel_last_offset{channel="$ch"} $n};
        }

        $c->render(text => join("\n", @lines) . "\n", format => 'text/plain');
    });

    return;
}

1;

__END__

=head1 NAME

Mojo::Centrifuge::ExampleApp - Shared HTTP routes for Mojo::Centrifuge demos and tests

=head1 SYNOPSIS

  use Mojo::Centrifuge::ExampleApp;

  Mojo::Centrifuge::ExampleApp::setup(
      hub     => $hub,
      api_key => $ENV{CENTRIFUGE_API_KEY},
  );

=head1 DESCRIPTION

Registers the C</publish> and C</api/*> HTTP endpoints on the currently
running Mojolicious application. Used by both C<example.pl> and
C<t/basic.t> so the route definitions don't drift.

=head1 FUNCTIONS

=head2 setup

  Mojo::Centrifuge::ExampleApp::setup(hub => $hub, api_key => $key);

Registers routes on C<Mojolicious-E<gt>app-E<gt>routes>. C<hub> must be a
L<Mojo::Centrifuge> instance. C<api_key> is the shared secret checked
via C<X-API-Key> or C<Authorization: Bearer <key>>.

Routes registered:

  POST /publish
  POST /api/publish
  POST /api/broadcast
  POST /api/history
  POST /api/presence
  POST /api/channels
  POST /api/info
  POST /api/history_remove
  POST /api/revoke
  POST /api/unsubscribe
  POST /api/disconnect
  GET  /api/status
  GET  /metrics

=head1 LICENSE

Same as Perl itself.

=cut
