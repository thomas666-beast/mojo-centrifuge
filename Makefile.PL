use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Mojo::Centrifuge',
    VERSION_FROM     => 'lib/Mojo/Centrifuge.pm',
    ABSTRACT         => 'Centrifugo-like realtime pub/sub server for Mojolicious',
    AUTHOR           => 'Your Name <you@example.com>',
    LICENSE          => 'perl_5',
    MIN_PERL_VERSION => '5.016',
    META_MERGE       => {
        'meta-spec' => { version => 2 },
        resources   => {
            repository => {
                type => 'git',
                url  => 'https://github.com/YOU/mojo-centrifuge.git',
                web  => 'https://github.com/YOU/mojo-centrifuge',
            },
            bugtracker => {
                web => 'https://github.com/YOU/mojo-centrifuge/issues',
            },
        },
    },
    PREREQ_PM => {
        'Mojolicious'  => '9.00',
        'Mojo::Redis'  => '3.00',
        'Mojo::Util'   => 0,
        'Mojo::JSON'   => 0,
        'Digest::SHA'  => 0,
        'MIME::Base64' => 0,
    },
    TEST_REQUIRES => {
        'Test::More' => '1.302',
        'Test::Mojo' => 0,
    },
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => '7.00',
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz' },
    clean => { FILES    => 'Mojo-Centrifuge-*' },
);
