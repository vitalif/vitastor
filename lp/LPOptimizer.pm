#!/usr/bin/perl
# Data distribution optimizer using linear programming (lp_solve)

package LPOptimizer;

use strict;
use IPC::Open2;

sub make_single
{
    my ($osd_tree) = @_;
    my $initial = all_combinations($osd_tree, 1)->[0];
    my $weight;
    my $all_weights = { map { %$_ } values %$osd_tree };
    for my $osd (@$initial)
    {
        $weight = $all_weights->{$osd} if !$weight || $all_weights->{$osd} < $weight;
    }
    return [
        { set => $initial, weight => $weight },
    ];
}

sub optimize_initial
{
    my ($osd_tree, $pg_count) = @_;
    my $all_weights = { map { %$_ } values %$osd_tree };
    my $pgs = all_combinations($osd_tree);
    my $pg_per_osd = {};
    for my $pg (@$pgs)
    {
        push @{$pg_per_osd->{$_}}, "pg_".join("_", @$pg) for @$pg;
    }
    my $lp = '';
    $lp .= "max: ".join(" + ", map { "pg_".join("_", @$_) } @$pgs).";\n";
    for my $osd (keys %$pg_per_osd)
    {
        $lp .= join(" + ", @{$pg_per_osd->{$osd}})." <= ".$all_weights->{$osd}.";\n";
    }
    for my $pg (@$pgs)
    {
        $lp .= "pg_".join("_", @$pg)." >= 0;\n";
    }
    $lp .= "int ".join(", ", map { "pg_".join("_", @$_) } @$pgs).";\n";
    my ($score, $weights) = lp_solve($lp);
    my $int_pgs = make_int_pgs($weights, $pg_count);
    my $eff = pg_list_space_efficiency($int_pgs, $osd_tree);
    my $total_weight = 0;
    $total_weight += $all_weights->{$_} for keys %$all_weights;
    return { score => $score, weights => $weights, int_pgs => $int_pgs, total_space => $eff * 3, space_eff => $eff * 3 / $total_weight };
}

sub make_int_pgs
{
    my ($weights, $pg_count) = @_;
    my $total_weight = 0;
    for my $pg_name (keys %$weights)
    {
        $total_weight += $weights->{$pg_name};
    }
    my $int_pgs = [];
    my $pg_left = $pg_count;
    for my $pg_name (keys %$weights)
    {
        my $n = int($weights->{$pg_name} / $total_weight * $pg_left + 0.5);
        for (my $i = 0; $i < $n; $i++)
        {
            push @$int_pgs, [ split /_/, substr($pg_name, 3) ];
        }
        $total_weight -= $weights->{$pg_name};
        $pg_left -= $n;
    }
    return $int_pgs;
}

sub lp_solve
{
    my ($lp) = @_;
    my ($pid, $out, $in, $result);
    $pid = open2($in, $out, 'lp_solve');
    print $out $lp;
    close $out;
    {
        local $/ = undef;
        $result = <$in>;
        close $in;
    }
    my $score = 0;
    my $weights = {};
    for my $line (split /\n/, $result)
    {
        if ($line =~ /^(^Value of objective function: ([\d\.]+)|Actual values of the variables:)\s*$/s)
        {
            if ($2)
            {
                $score = $2;
            }
            next;
        }
        my ($k, $v) = split /\s+/, $line;
        if ($v != 0)
        {
            $weights->{$k} = $v;
        }
    }
    return ($score, $weights);
}

sub get_int_pg_weights
{
    my ($prev_int_pgs, $osd_tree) = @_;
    my $space = pg_list_space_efficiency($prev_int_pgs, $osd_tree);
    my $prev_weights = {};
    my $count = 0;
    for my $pg (@$prev_int_pgs)
    {
        $prev_weights->{'pg_'.join('_', @$pg)}++;
        $count++;
    }
    for my $pg_name (keys %$prev_weights)
    {
        $prev_weights->{$pg_name} *= $space / $count;
    }
    return $prev_weights;
}

# Try to minimize data movement
sub optimize_change
{
    my ($prev_int_pgs, $osd_tree) = @_;
    my $pg_count = scalar(@$prev_int_pgs);
    my $prev_weights = {};
    my $prev_pg_per_osd = {};
    for my $pg (@$prev_int_pgs)
    {
        $prev_weights->{"pg_".join("_", @$pg)}++;
        push @{$prev_pg_per_osd->{$_}}, "pg_".join("_", @$pg) for @$pg;
    }
    # Get all combinations
    my $pgs = all_combinations($osd_tree);
    my $pg_per_osd = {};
    for my $pg (@$pgs)
    {
        push @{$pg_per_osd->{$_}}, "pg_".join("_", @$pg) for @$pg;
    }
    # Penalize PGs based on their similarity to old PGs
    my $intersect = {};
    for my $pg_name (keys %$prev_weights)
    {
        my @pg = split /_/, substr($pg_name, 'pg_');
        $intersect->{$pg[0].'::'} = $intersect->{':'.$pg[1].':'} = $intersect->{'::'.$pg[2]} = 1;
        $intersect->{$pg[0].'::'.$pg[2]} = $intersect->{':'.$pg[1].':'.$pg[2]} = $intersect->{$pg[0].':'.$pg[1].':'} = 2;
    }
    my $move_weights = {};
    for my $pg (@$pgs)
    {
        $move_weights->{'pg_'.join('_', @$pg)} =
            $intersect->{$pg->[0].'::'} || $intersect->{':'.$pg->[1].':'} || $intersect->{'::'.$pg->[2]} ||
            $intersect->{$pg->[0].'::'.$pg->[2]} || $intersect->{':'.$pg->[1].':'.$pg->[2]} || $intersect->{$pg->[0].':'.$pg->[1].':'} || 3;
    }
    # Calculate total weight - old PG weights
    my $pg_names = [ map { 'pg_'.join('_', @$_) } @$pgs ];
    my $all_weights = { map { %$_ } values %$osd_tree };
    my $tw = 0;
    $tw += $all_weights->{$_} for keys %$all_weights;
    $tw = $tw/3;
    # Generate the LP problem
    my $lp = "min: ".join(" + ", map { $move_weights->{$_} . ' * ' . ($prev_weights->{$_} ? "add_$_" : "$_") } @$pg_names).";\n";
    $lp .= join(" + ", map { $prev_weights->{$_} ? "add_$_ - del_$_" : $_ } @$pg_names)." = 0;\n";
    for my $osd (keys %$pg_per_osd)
    {
        my $w = $all_weights->{$osd};
        my @s;
        for my $pg (@{$pg_per_osd->{$osd}})
        {
            if ($prev_weights->{$pg})
            {
                push @s, "add_$pg - del_$pg";
            }
            else
            {
                push @s, $pg;
            }
        }
        $lp .= join(" + ", @s)." <= ".int($all_weights->{$osd}/$tw*$pg_count - scalar(@{$prev_pg_per_osd->{$osd} || []})).";\n";
    }
    my @sec;
    for my $pg (@$pg_names)
    {
        if ($prev_weights->{$pg})
        {
            push @sec, "add_$pg", "del_$pg";
            # Can't add or remove less than zero
            $lp .= "add_$pg >= 0;\n";
            $lp .= "del_$pg >= 0;\n";
            # Can't remove more than the PG already has
            $lp .= "add_$pg - del_$pg >= -".$prev_weights->{$pg}.";\n";
        }
        else
        {
            push @sec, $pg;
            $lp .= "$pg >= 0;\n";
        }
    }
    $lp .= "int ".join(", ", @sec).";\n";
    # Solve it
    my ($score, $result) = lp_solve($lp);
    # Generate the new distribution
    my $weights = { %$prev_weights };
    for my $k (keys %$result)
    {
        if ($k =~ /^add_/s)
        {
            $weights->{substr($k, 4)} += $result->{$k};
        }
        elsif ($k =~ /^del_/s)
        {
            $weights->{substr($k, 4)} -= $result->{$k};
        }
        else
        {
            $weights->{$k} = $result->{$k};
        }
    }
    for my $k (keys %$weights)
    {
        delete $weights->{$k} if !$weights->{$k};
    }
    my $int_pgs = make_int_pgs($weights, scalar @$prev_int_pgs);
    # Align them with most similar previous PGs
    my $new_pgs = align_pgs($prev_int_pgs, $int_pgs);
    my $differs = 0;
    my $osd_differs = 0;
    for my $i (0..$#$new_pgs)
    {
        if (join('_', @{$new_pgs->[$i]}) ne join('_', @{$prev_int_pgs->[$i]}))
        {
            $differs++;
        }
        for my $j (0..2)
        {
            if ($new_pgs->[$i]->[$j] ne $prev_int_pgs->[$i]->[$j])
            {
                $osd_differs++;
            }
        }
    }
    my $eff = pg_list_space_efficiency($new_pgs, $osd_tree);
    my $total_weight = 0;
    $total_weight += $all_weights->{$_} for keys %$all_weights;
    return {
        prev_pgs => $prev_int_pgs,
        score => $score,
        weights => $weights,
        int_pgs => $new_pgs,
        differs => $differs,
        osd_differs => $osd_differs,
        space => $eff,
        total_space => $total_weight,
    };
}

sub print_change_stats
{
    my ($retval) = @_;
    my $new_pgs = $retval->{int_pgs};
    my $prev_int_pgs = $retval->{prev_pgs};
    for my $i (0..$#$new_pgs)
    {
        if (join('_', @{$new_pgs->[$i]}) ne join('_', @{$prev_int_pgs->[$i]}))
        {
            print "pg $i: ".join(' ', @{$prev_int_pgs->[$i]})." -> ".join(' ', @{$new_pgs->[$i]})."\n";
        }
    }
    printf("Data movement: ".$retval->{differs}." pgs, ".$retval->{osd_differs}." pg-osds = %.2f %%\n", $retval->{osd_differs} / @$prev_int_pgs / 3 * 100);
    printf("Total space: %.2f TB, space efficiency: %.2f %%\n", $retval->{space} * 3, $retval->{space} * 3 / $retval->{total_space} * 100);
}

sub align_pgs
{
    my ($prev_int_pgs, $int_pgs) = @_;
    my $aligned_pgs = [];
    put_aligned_pgs($aligned_pgs, $int_pgs, $prev_int_pgs, sub
    {
        my ($pg) = @_;
        return (join(':', @$pg));
    });
    put_aligned_pgs($aligned_pgs, $int_pgs, $prev_int_pgs, sub
    {
        my ($pg) = @_;
        return ($pg->[0].'::'.$pg->[2], ':'.$pg->[1].':'.$pg->[2], $pg->[0].':'.$pg->[1].':');
    });
    put_aligned_pgs($aligned_pgs, $int_pgs, $prev_int_pgs, sub
    {
        my ($pg) = @_;
        return ($pg->[0].'::', ':'.$pg->[1].':', '::'.$pg->[2]);
    });
    my $free_slots = [ grep { !$aligned_pgs->[$_] } 0..$#$prev_int_pgs ];
    for my $pg (@$int_pgs)
    {
        die "Can't place unaligned PG" if !@$free_slots;
        $aligned_pgs->[shift @$free_slots] = $pg;
    }
    return $aligned_pgs;
}

sub put_aligned_pgs
{
    my ($aligned_pgs, $int_pgs, $prev_int_pgs, $keygen) = @_;
    my $prev_indexes = {};
    for (my $i = 0; $i < @$prev_int_pgs; $i++)
    {
        for my $k ($keygen->($prev_int_pgs->[$i]))
        {
            push @{$prev_indexes->{$k}}, $i;
        }
    }
    PG: for (my $i = $#$int_pgs; $i >= 0; $i--)
    {
        my $pg = $int_pgs->[$i];
        my @keys = $keygen->($int_pgs->[$i]);
        for my $k (@keys)
        {
            while (@{$prev_indexes->{$k} || []})
            {
                my $idx = shift @{$prev_indexes->{$k}};
                if (!$aligned_pgs->[$idx])
                {
                    $aligned_pgs->[$idx] = $pg;
                    splice @$int_pgs, $i, 1;
                    next PG;
                }
            }
        }
    }
}

sub all_combinations
{
    my ($osd_tree, $count, $ordered) = @_;
    my $hosts = [ sort keys %$osd_tree ];
    my $osds = { map { $_ => [ sort keys %{$osd_tree->{$_}} ] } keys %$osd_tree };
    my $h = [ 0, 1, 2 ];
    my $o = [ 0, 0, 0 ];
    my $r = [];
    while (!$count || $count < 0 || @$r < $count)
    {
        my $inc;
        if ($h->[2] != $h->[1] && $h->[2] != $h->[0] && $h->[1] != $h->[0])
        {
            push @$r, [ map { $osds->{$hosts->[$h->[$_]]}->[$o->[$_]] } 0..$#$h ];
            $inc = 2;
            while ($inc >= 0)
            {
                $o->[$inc]++;
                if ($o->[$inc] >= scalar @{$osds->{$hosts->[$h->[$inc]]}})
                {
                    $o->[$inc] = 0;
                    $inc--;
                }
                else
                {
                    last;
                }
            }
        }
        else
        {
            $inc = -1;
        }
        if ($inc < 0)
        {
            # no osds left in current host combination, select next one
            $o = [ 0, 0, 0 ];
            $h->[2]++;
            if ($h->[2] >= scalar @$hosts)
            {
                $h->[1]++;
                $h->[2] = $ordered ? $h->[1]+1 : 0;
                if (($ordered ? $h->[2] : $h->[1]) >= scalar @$hosts)
                {
                    $h->[0]++;
                    $h->[1] = $ordered ? $h->[0]+1 : 0;
                    $h->[2] = $ordered ? $h->[1]+1 : 0;
                    if (($ordered ? $h->[2] : $h->[0]) >= scalar @$hosts)
                    {
                        last;
                    }
                }
            }
        }
    }
    return $r;
}

sub pg_weights_space_efficiency
{
    my ($weights, $pg_count, $osd_tree) = @_;
    my $per_osd = {};
    for my $pg_name (keys %$weights)
    {
        for my $osd (split /_/, substr($pg_name, 3))
        {
            $per_osd->{$osd}++;
        }
    }
    return pg_per_osd_space_efficiency($per_osd, $pg_count, $osd_tree);
}

sub pg_list_space_efficiency
{
    my ($pgs, $osd_tree) = @_;
    my $per_osd = {};
    for my $pg (@$pgs)
    {
        for my $osd (@$pg)
        {
            $per_osd->{$osd}++;
        }
    }
    return pg_per_osd_space_efficiency($per_osd, scalar @$pgs, $osd_tree);
}

sub pg_per_osd_space_efficiency
{
    my ($per_osd, $pg_count, $osd_tree) = @_;
    my $all_weights = { map { %$_ } values %$osd_tree };
    # each PG gets randomly selected in 1/N cases
    # => there are x PGs per OSD
    # => an OSD is selected in x/N cases
    # => total space * x/N <= OSD weight
    # => total space <= OSD weight * N/x
    my $space = undef;
    for my $osd (keys %$per_osd)
    {
        my $space_estimate = $all_weights->{$osd} * $pg_count / $per_osd->{$osd};
        if (!defined $space || $space > $space_estimate)
        {
            $space = $space_estimate;
        }
    }
    return $space;
}

1;
__END__
