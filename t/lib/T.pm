package T;
use strict;
use warnings;
use Exporter 'import';
use Test::More;
use Mojo::Centrifuge;
use Mojo::Centrifuge::Engine::Memory;
use Mojo::Centrifuge::Scheduler::Manual;
use Mojo::Centrifuge::JWT;
use Mojo::Centrifuge::Util qw(encode_json decode_json);

our @EXPORT = qw(make_hub conn token find_reply);
our $SECRET = 'test-secret';

# A hub on a fake clock with a standard set of namespaces.
sub make_hub {
    my (%o) = @_;
    my $sched  = $o{scheduler} || Mojo::Centrifuge::Scheduler::Manual->new;
    my $now    = sub { $sched->now };
    my $engine = $o{engine} || Mojo::Centrifuge::Engine::Memory->new(now => $now);
    my %std = (
        default => {
            history_size => 10, history_ttl => 300, force_recovery => 1,
            presence => 1, join_leave => 1,
            allow_subscribe_for_client => 1, allow_publish_for_client => 1,
            allow_presence_for_client => 1, allow_history_for_client => 1,
        },
        namespaces => {
            private => { presence => 1, allow_publish_for_subscriber => 1, allow_presence_for_subscriber => 1 },
            inbox   => { allow_user_limited_channels => 1 },
            plain   => { allow_subscribe_for_client => 1, allow_publish_for_client => 1 },
            small   => { history_size => 3, history_ttl => 300, force_recovery => 1, allow_subscribe_for_client => 1 },
        },
    );
    my $hub = Mojo::Centrifuge->new(
        engine => $engine, scheduler => $sched, secret => $SECRET, api_key => 'k',
        namespaces => $o{namespaces} || \%std,
        (map { exists $o{$_} ? ($_ => $o{$_}) : () } qw(hooks allow_anonymous ping_interval pong_timeout presence_ttl expired_close_delay node_id connect_timeout)),
    );
    return wantarray ? ($hub, $sched, $engine) : $hub;
}

sub token {
    my ($sched, %claims) = @_;
    my $jwt = Mojo::Centrifuge::JWT->new(secret => $SECRET, now => sub { $sched->now });
    return $jwt->encode({ sub => 'u1', %claims });
}

sub find_reply {
    my ($id, @frames) = @_;
    my ($f) = grep { defined $_->{id} && $_->{id} == $id } @frames;
    return $f;
}

package T::Conn;
use strict;
use warnings;
use Mojo::Centrifuge::Util qw(encode_json decode_json);

sub new {
    my ($class, $hub) = @_;
    my $self = bless { frames => [], raw => [], close => undef, hub => $hub, n => 0 }, $class;
    $self->{client} = $hub->new_client(
        send  => sub { push @{ $self->{raw} }, $_[0]; push @{ $self->{frames} }, decode_json($_[0]) },
        close => sub { $self->{close} = [@_] },
    );
    return $self;
}
sub client { $_[0]{client} }
sub closed { $_[0]{close} }
sub close_code { $_[0]{close} ? $_[0]{close}[0] : undef }
sub close_info { $_[0]{close} ? decode_json($_[0]{close}[1]) : undef }

# Send one or more commands in a single frame; returns the ids used.
sub cmd {
    my ($self, @cmds) = @_;
    my @ids;
    my @lines = map { my %c = %$_; $c{id} //= ++$self->{n}; push @ids, $c{id}; encode_json(\%c) } @cmds;
    $self->{client}->handle_frame(join "\n", @lines);
    return wantarray ? @ids : $ids[0];
}
sub raw   { my $s = shift; my @r = @{ $s->{raw} }; return join "\n", @r }
sub take  { my $s = shift; my @f = @{ $s->{frames} }; $s->{frames} = []; $s->{raw} = []; return @f }
sub peek  { @{ $_[0]{frames} } }

# connect with a JWT and return the connect result
sub connect {
    my ($self, $token, %extra) = @_;
    my $id = $self->cmd({ connect => { token => $token, %extra } });
    my ($f) = grep { defined $_->{id} && $_->{id} == $id } $self->take;
    return $f;
}

1;
