package Mojo::Centrifuge::Proxy::HTTP;
use Mojo::Base -base;
use Mojo::UserAgent;

# Turns "POST JSON to my backend" into a hook for Mojo::Centrifuge:
#
#   my $connect = Mojo::Centrifuge::Proxy::HTTP->new(endpoint => 'http://backend/centrifugo/connect');
#   Mojo::Centrifuge->new(hooks => { connect => $connect->hook }, ...);
#
# The backend receives the request as JSON (client, transport, protocol, user,
# channel, data, ...) and answers with Centrifugo's proxy format:
#   {"result": {...}}  |  {"error": {"code": 103, "message": "..."}}  |  {"disconnect": {"code": 3507, "reason": "..."}}

has 'endpoint';
has headers => sub { {} };
has timeout => 5;
has ua      => sub { Mojo::UserAgent->new };

sub hook {
    my $self = shift;
    return sub {
        my ($req, $done) = @_;
        my $ua = $self->ua;
        $ua->request_timeout($self->timeout)->connect_timeout($self->timeout);
        $ua->post($self->endpoint => $self->headers => json => $req => sub {
            my ($ua, $tx) = @_;
            my $err = $tx->error;
            return $done->({ error => { code => 100, message => 'internal server error', temporary => 1 } })
                if $err;    # network failure or non-2xx
            my $json = $tx->res->json;
            $done->(ref $json eq 'HASH' ? $json : { error => { code => 100, message => 'internal server error', temporary => 1 } });
        });
    };
}

1;
