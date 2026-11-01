package Mojo::Centrifuge::Scheduler::Manual;
use strict;
use warnings;

# Deterministic fake clock + timers for tests: nothing fires until ->advance.
sub new {
    my ($class, $start) = @_;
    return bless { now => $start // 1_700_000_000, next => 1, t => {} }, $class;
}

sub now { $_[0]{now} }

sub timer {
    my ($s, $sec, $cb) = @_;
    my $id = $s->{next}++;
    $s->{t}{$id} = { at => $s->{now} + $sec, cb => $cb, every => 0 };
    return $id;
}

sub recurring {
    my ($s, $sec, $cb) = @_;
    die "recurring interval must be > 0\n" unless $sec > 0;
    my $id = $s->{next}++;
    $s->{t}{$id} = { at => $s->{now} + $sec, cb => $cb, every => $sec };
    return $id;
}

sub remove { delete $_[0]{t}{$_[1]} if defined $_[1] }

sub pending { scalar keys %{ $_[0]{t} } }

sub advance {
    my ($s, $sec) = @_;
    my $end = $s->{now} + $sec;
    while (1) {
        my ($id) = sort { $s->{t}{$a}{at} <=> $s->{t}{$b}{at} || $a <=> $b }
                   grep { $s->{t}{$_}{at} <= $end } keys %{ $s->{t} };
        last unless defined $id;
        my $t = $s->{t}{$id};
        $s->{now} = $t->{at} if $t->{at} > $s->{now};
        if ($t->{every}) { $t->{at} += $t->{every} } else { delete $s->{t}{$id} }
        $t->{cb}->();
    }
    $s->{now} = $end;
}

1;
