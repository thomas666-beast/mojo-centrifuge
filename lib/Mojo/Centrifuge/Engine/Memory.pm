package Mojo::Centrifuge::Engine::Memory;
use strict;
use warnings;
use Digest::SHA ();
use Mojo::Centrifuge::Util qw(decode_json pub_payload);

# In-process engine: history, presence and the pub/sub bus all live in memory.
# Fine for development, tests and single-process deployments. Several hubs may
# share one instance (that is how the tests simulate a cluster).
my $counter = 0;

sub new {
    my ($class, %a) = @_;
    return bless {
        now          => $a{now} || sub { time },
        streams      => {},    # ch => {epoch, offset, expires}
        hist         => {},    # ch => [{offset, d, i}, ...] oldest first
        hist_expires => {},
        pres         => {},    # ch => {client => {info, exp}}
        bus          => {},    # bus name => {owner => cb}
    }, $class;
}

sub _new_epoch {
    return substr Digest::SHA::sha1_hex(join '-', $$, time, rand(), ++$counter), 0, 8;
}

sub _gc {
    my ($self, $ch) = @_;
    my $now = $self->{now}->();
    if (defined $self->{hist_expires}{$ch} && $self->{hist_expires}{$ch} <= $now) {
        delete $self->{hist}{$ch};
        delete $self->{hist_expires}{$ch};
    }
    my $m = $self->{streams}{$ch};
    if ($m && defined $m->{expires} && $m->{expires} <= $now) {
        delete $self->{streams}{$ch};
        delete $self->{hist}{$ch};
        delete $self->{hist_expires}{$ch};
    }
}

sub _meta {
    my ($self, $ch, $o) = @_;
    $self->_gc($ch);
    my $m = $self->{streams}{$ch} ||= { epoch => _new_epoch(), offset => 0 };
    $m->{expires} = $self->{now}->() + ($o->{meta_ttl} || 30 * 86400);
    return $m;
}

# Store in the stream (when history is enabled) and publish on the bus, as one step.
# Returns ($offset, $epoch); (0, '') when the channel has no stream.
sub publish {
    my ($self, $ch, $data_json, $info_json, $o) = @_;
    $o ||= {};
    my ($offset, $epoch) = (0, '');
    if (($o->{history_size} // 0) > 0 && ($o->{history_ttl} // 0) > 0) {
        my $m = $self->_meta($ch, $o);
        $offset = ++$m->{offset};
        $epoch  = $m->{epoch};
        my $h = $self->{hist}{$ch} ||= [];
        push @$h, { offset => $offset, d => $data_json, i => $info_json };
        shift @$h while @$h > $o->{history_size};
        $self->{hist_expires}{$ch} = $self->{now}->() + $o->{history_ttl};
    }
    $self->bus_publish(Mojo::Centrifuge::Util::bus_name($ch), pub_payload($ch, $offset, $epoch, $data_json, $info_json));
    return ($offset, $epoch);
}

# Current top offset + epoch; creates the stream position if it does not exist yet
# so a subscriber's epoch stays valid for the first publication.
sub stream_info {
    my ($self, $ch, $o) = @_;
    return (0, '') unless ($o->{history_size} // 0) > 0 && ($o->{history_ttl} // 0) > 0;
    my $m = $self->_meta($ch, $o);
    return ($m->{offset}, $m->{epoch});
}

# (\@publications oldest-first, $top_offset, $epoch)
sub history {
    my ($self, $ch) = @_;
    $self->_gc($ch);
    my $m = $self->{streams}{$ch};
    my @pubs = map {
        my $i = decode_json($_->{i} // 'null');
        { offset => $_->{offset}, data => decode_json($_->{d}), (ref $i eq 'HASH' ? (info => $i) : ()) }
    } @{ $self->{hist}{$ch} || [] };
    return (\@pubs, $m ? $m->{offset} : 0, $m ? $m->{epoch} : '');
}

sub history_remove {
    my ($self, $ch) = @_;
    delete $self->{hist}{$ch};
    delete $self->{hist_expires}{$ch};
    return 1;
}

sub presence_add {
    my ($self, $ch, $client, $info_json, $ttl) = @_;
    $self->{pres}{$ch}{$client} = { info => $info_json, exp => $self->{now}->() + $ttl };
    return 1;
}

sub presence_remove {
    my ($self, $ch, $client) = @_;
    my $p = $self->{pres}{$ch} or return 1;
    delete $p->{$client};
    delete $self->{pres}{$ch} unless keys %$p;
    return 1;
}

sub presence {
    my ($self, $ch) = @_;
    my $now = $self->{now}->();
    my $p = $self->{pres}{$ch} || {};
    for my $id (keys %$p) { delete $p->{$id} if $p->{$id}{exp} <= $now }
    delete $self->{pres}{$ch} unless keys %$p;
    return { map { $_ => decode_json($p->{$_}{info}) } keys %$p };
}

# ---- bus ------------------------------------------------------------------
sub bus_subscribe {
    my ($self, $name, $cb, $owner) = @_;
    $self->{bus}{$name}{"$owner"} = $cb;
}

sub bus_unsubscribe {
    my ($self, $name, $owner) = @_;
    my $subs = $self->{bus}{$name} or return;
    delete $subs->{"$owner"};
    delete $self->{bus}{$name} unless keys %$subs;
}

sub bus_publish {
    my ($self, $name, $json) = @_;
    my $subs = $self->{bus}{$name} or return;
    for my $cb (map { $subs->{$_} } sort keys %$subs) {
        eval { $cb->($name, $json); 1 } or warn "bus callback failed: $@";
    }
}

1;
