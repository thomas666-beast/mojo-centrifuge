requires 'perl', '5.016';
requires 'Mojolicious', '9.00';
requires 'Mojo::Redis',  '3.00';
requires 'Mojo::Util';
requires 'Mojo::JSON';
requires 'Digest::SHA';
requires 'MIME::Base64';

on test => sub {
    requires 'Test::More', '1.302';
    requires 'Test::Mojo';
};

on develop => sub {
    requires 'Test::Pod';
    requires 'Test::Pod::Coverage';
};
