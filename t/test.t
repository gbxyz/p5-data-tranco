#!/usr/bin/perl
use Test::More;
use strict;
use warnings;

require_ok 'Data::Tranco';

ok Data::Tranco->random_domain;
ok Data::Tranco->top_domain;

done_testing;