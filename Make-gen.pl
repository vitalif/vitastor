#!/usr/bin/perl
# FIXME: Use CMake :)

use strict;

my $deps = {};
for my $line (split /\n/, `grep '^#include "' *.cpp *.h`)
{
    if ($line =~ /^([^:]+):\#include "([^"]+)"/s)
    {
        $deps->{$1}->{$2} = 1;
    }
}

my $added;
do
{
    $added = 0;
    for my $file (keys %$deps)
    {
        for my $dep (keys %{$deps->{$file}})
        {
            if ($deps->{$dep})
            {
                for my $subdep (keys %{$deps->{$dep}})
                {
                    if (!$deps->{$file}->{$subdep})
                    {
                        $added = 1;
                        $deps->{$file}->{$subdep} = 1;
                    }
                }
            }
        }
    }
} while ($added);

for my $file (sort keys %$deps)
{
    if ($file =~ /\.cpp$/)
    {
        my $obj = $file;
        $obj =~ s/\.cpp$/.o/s;
        print "$obj: $file ".join(" ", sort keys %{$deps->{$file}})."\n";
        print "\tg++ \$(CXXFLAGS) -c -o \$\@ \$\<\n";
    }
}
