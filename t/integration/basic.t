use strict;
use warnings;
use Test::More;

plan skip_all 'set CENTRIFUGE_TEST_REDIS=redis://127.0.0.1:6379/0 to run'
    unless $ENV{CENTRIFUGE_TEST_REDIS};

# Full ws_client.pl-style scenario lives here in Phase 1.
pass 'placeholder — integration scenarios land in Phase 1';
done_testing;
