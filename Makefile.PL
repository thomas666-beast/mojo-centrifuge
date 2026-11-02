use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Mojo::Centrifuge',
    VERSION_FROM     => 'lib/Mojo/Centrifuge.pm',
    ABSTRACT         => 'Centrifugo-like realtime pub/sub server for Mojolicious',
    AUTHOR           => 'Thomas <selfworker112@mailfence.com>',
    LICENSE          => 'perl_5',
    MIN_PERL_VERSION => '5.016',
    META_MERGE       => {
        'meta-spec' => { version => 2 },
        resources   => {
            repository => {
                type => 'git',
                url  => 'https://github.com/thomas666-beast/mojo-centrifuge.git',
                web  => 'https://github.com/thomas666-beast/mojo-centrifuge',
            },
            bugtracker => {
                web => 'https://github.com/thomas666-beast/mojo-centrifuge/issues',
            },
        },
    },
    PM => {
        'lib/Mojo/Centrifuge.pm'              => '$(INST_LIB)/Mojo/Centrifuge.pm',
        'lib/Mojo/Centrifuge/Auth.pm'         => '$(INST_LIB)/Mojo/Centrifuge/Auth.pm',
        'lib/Mojo/Centrifuge/Protocol.pm'     => '$(INST_LIB)/Mojo/Centrifuge/Protocol.pm',
        'lib/Mojo/Centrifuge/ExampleApp.pm'   => '$(INST_LIB)/Mojo/Centrifuge/ExampleApp.pm',
    },
    PREREQ_PM => {
        'Mojolicious'  => '9.00',
        'Mojo::Redis'  => '3.00',
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
