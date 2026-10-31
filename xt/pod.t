use strict;
use warnings;
use Test::More;

plan skip_all => 'set AUTHOR_TESTING=1 to run' unless $ENV{AUTHOR_TESTING};

eval { require Test::Pod; Test::Pod->import; 1 }
    or plan skip_all => 'Test::Pod not installed';

all_pod_files_ok();
