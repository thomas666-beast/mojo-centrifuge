package Mojo::Centrifuge::Namespaces;
use strict;
use warnings;
use Carp ();

# Channel "chat:room1" belongs to namespace "chat"; a channel without ":" uses
# the default options. Namespaces do NOT inherit from the default (like Centrifugo).
my %DEFAULTS = (
    history_size                  => 0,
    history_ttl                   => 0,    # seconds
    presence                      => 0,
    join_leave                    => 0,
    force_recovery                => 0,
    force_positioning             => 0,
    allow_subscribe_for_client    => 0,
    allow_publish_for_client      => 0,
    allow_publish_for_subscriber  => 0,
    allow_presence_for_client     => 0,
    allow_presence_for_subscriber => 0,
    allow_history_for_client      => 0,
    allow_history_for_subscriber  => 0,
    allow_user_limited_channels   => 0,
    subscribe_proxy               => 0,
    publish_proxy                 => 0,
);
my %INT = map { $_ => 1 } qw(history_size history_ttl);

sub new {
    my ($class, %a) = @_;
    my $self = bless {
        max      => $a{channel_max_length} // 255,
        meta_ttl => $a{history_meta_ttl} // 30 * 86400,
        ns       => {},
    }, $class;
    $self->{default} = _norm('default', $a{default} || {});
    my $ns = $a{namespaces} || {};
    Carp::croak('namespaces must be a hash') unless ref $ns eq 'HASH';
    for my $name (sort keys %$ns) {
        Carp::croak("invalid namespace name '$name'") unless $name =~ /\A[A-Za-z0-9_.-]+\z/;
        $self->{ns}{$name} = _norm($name, $ns->{$name});
    }
    return $self;
}

sub _norm {
    my ($name, $in) = @_;
    Carp::croak("options for '$name' must be a hash") unless ref $in eq 'HASH';
    for my $k (keys %$in) { Carp::croak("unknown option '$k' in namespace '$name'") unless exists $DEFAULTS{$k} }
    my %o = (%DEFAULTS, %$in);
    for my $k (keys %o) {
        if ($INT{$k}) {
            Carp::croak("'$k' in '$name' must be a non-negative integer") unless defined $o{$k} && $o{$k} =~ /\A\d+\z/;
            $o{$k} += 0;
        }
        else { $o{$k} = $o{$k} ? 1 : 0 }
    }
    Carp::croak("'$name': history_size and history_ttl must be set together")
        if ($o{history_size} > 0) != ($o{history_ttl} > 0);
    my $has_history = $o{history_size} > 0;
    Carp::croak("'$name': force_recovery/force_positioning need history_size and history_ttl")
        if ($o{force_recovery} || $o{force_positioning}) && !$has_history;
    $o{history}     = $has_history ? 1 : 0;
    $o{recoverable} = $o{force_recovery} ? 1 : 0;
    $o{positioned}  = ($o{force_recovery} || $o{force_positioning}) ? 1 : 0;
    return \%o;
}

sub meta_ttl { $_[0]{meta_ttl} }

# ($opts, $namespace_name) or (undef, 'bad_request'|'unknown')
sub resolve {
    my ($self, $ch) = @_;
    return (undef, 'bad_request')
        unless defined $ch && !ref $ch && length $ch && length $ch <= $self->{max};
    if ($ch =~ /\A([^:]*):/) {
        my $ns = $1;
        return (undef, 'unknown') unless length $ns && exists $self->{ns}{$ns};
        return ($self->{ns}{$ns}, $ns);
    }
    return ($self->{default}, '');
}

# undef when $ch is not a user-limited channel, else 1/0
sub user_limited {
    my ($self, $ch, $user) = @_;
    my $i = index $ch, '#';
    return undef if $i < 0;
    return 0 unless defined $user && length $user;
    return (grep { $_ eq $user } split /,/, substr($ch, $i + 1)) ? 1 : 0;
}

# Engine options for publishing/reading a channel's stream.
sub history_opts {
    my ($self, $opts) = @_;
    return { history_size => $opts->{history_size}, history_ttl => $opts->{history_ttl}, meta_ttl => $self->{meta_ttl} };
}

1;
