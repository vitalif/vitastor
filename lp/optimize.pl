#!/usr/bin/perl

use strict;
use lib qw(.);
use LPOptimizer;

my $osd_tree = {
    100 => {
        7 => 3.63869,
    },
    200 => {
        5 => 3.63869,
        6 => 3.63869,
    },
    300 => {
        10 => 3.46089,
        11 => 3.46089,
        12 => 3.46089,
    },
    400 => {
        1 => 3.49309,
        2 => 3.49309,
        3 => 3.49309,
    },
    500 => {
        4 => 3.58498,
        9 => 3.63869,
#        8 => 3.58589,
    },
};

my $prev = LPOptimizer::optimize_initial($osd_tree, 256);
my $int = LPOptimizer::get_int_pg_weights($prev->{int_pgs}, $osd_tree);
$osd_tree->{500}->{8} = 3.58589;
LPOptimizer::optimize_change($prev->{int_pgs}, $osd_tree);
