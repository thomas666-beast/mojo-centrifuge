use strict;
use warnings;
use Test::More;

plan skip_all => 'set AUTHOR_TESTING=1 to run' unless $ENV{AUTHOR_TESTING};

eval { require Test::Pod::Coverage; Test::Pod::Coverage->import; 1 }
    or plan skip_all => 'Test::Pod::Coverage not installed';

all_pod_coverage_ok({ trustme => [qr/^(new|has|_)/] });
