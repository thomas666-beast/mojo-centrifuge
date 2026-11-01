package Mojo::Centrifuge::Scheduler::IOLoop;
use strict;
use warnings;

sub new { return bless {}, shift }

sub timer {
    my ($self, $sec, $cb) = @_;
    require Mojo::IOLoop;
    return Mojo::IOLoop->timer($sec => sub { $cb->() });
}

sub recurring {
    my ($self, $sec, $cb) = @_;
    require Mojo::IOLoop;
    return Mojo::IOLoop->recurring($sec => sub { $cb->() });
}

sub remove {
    my ($self, $id) = @_;
    return unless defined $id;
    require Mojo::IOLoop;
    Mojo::IOLoop->remove($id);
}

1;
