# Install as /usr/share/perl5/PVE/Storage/Custom/VitastorPlugin.pm

# Proxmox Vitastor Driver
# Copyright (c) Vitaliy Filippov, 2021+
# License: VNPL-1.1 or GNU AGPLv3.0

package PVE::Storage::Custom::VitastorPlugin;

use strict;
use warnings;

use JSON;

use PVE::Storage::Plugin;
use PVE::Tools qw(run_command);

use base qw(PVE::Storage::Plugin);

if (@PVE::Storage::Plugin::SHARED_STORAGE)
{
    push @PVE::Storage::Plugin::SHARED_STORAGE, 'vitastor';
}

sub api
{
    # Trick it :)
    return PVE::Storage->APIVER;
}

sub run_cli
{
    my ($scfg, $cmd, %args) = @_;
    my $retval;
    my $stderr = '';
    my $errmsg = $args{errmsg} ? $args{errmsg}.": " : "vitastor-cli error: ";
    my $json = delete $args{json};
    $json = 1 if !defined $json;
    my $binary = delete $args{binary};
    $binary = '/usr/bin/vitastor-cli' if !defined $binary;
    if (!exists($args{errfunc}))
    {
        $args{errfunc} = sub
        {
            my $line = shift;
            print STDERR $line;
            *STDERR->flush();
            $stderr .= $line;
        };
    }
    if (!exists($args{outfunc}))
    {
        $retval = '';
        $args{outfunc} = sub { $retval .= shift };
        if ($json)
        {
            unshift @$cmd, '--json';
        }
    }
    if ($scfg->{vitastor_etcd_address})
    {
        unshift @$cmd, '--etcd_address', $scfg->{vitastor_etcd_address};
    }
    if ($scfg->{vitastor_config_path})
    {
        unshift @$cmd, '--config_path', $scfg->{vitastor_config_path};
    }
    unshift @$cmd, $binary;
    eval { run_command($cmd, %args); };
    if (my $err = $@)
    {
        die "Error invoking vitastor-cli: $err";
    }
    if (defined $retval)
    {
        # untaint
        $retval =~ /^(.*)$/s;
        if ($json)
        {
            eval { $retval = JSON::decode_json($1); };
            if ($@)
            {
                die "vitastor-cli returned bad JSON: $@";
            }
        }
        else
        {
            $retval = $1;
        }
    }
    return $retval;
}

# Configuration

sub type
{
    return 'vitastor';
}

sub plugindata
{
    return {
        content => [ { images => 1, rootdir => 1 }, { images => 1 } ],
    };
}

sub properties
{
    return {
        vitastor_etcd_address => {
            description => 'IP address(es) of etcd.',
            type => 'string',
            format => 'pve-storage-portal-dns-list',
        },
        vitastor_etcd_prefix => {
            description => 'Prefix for Vitastor etcd metadata',
            type => 'string',
        },
        vitastor_config_path => {
            description => 'Path to Vitastor configuration file',
            type => 'string',
        },
        vitastor_prefix => {
            description => 'Image name prefix',
            type => 'string',
        },
        vitastor_pool => {
            description => 'Default pool to use for images',
            type => 'string',
        },
        vitastor_nbd => {
            description => 'Use kernel NBD devices (slower)',
            type => 'boolean',
        },
    };
}

sub options
{
    return {
        shared => { optional => 1 },
        content => { optional => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        vitastor_etcd_address => { optional => 1 },
        vitastor_etcd_prefix => { optional => 1 },
        vitastor_config_path => { optional => 1 },
        vitastor_prefix => { optional => 1 },
        vitastor_pool => {},
        vitastor_nbd => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname
{
    my ($class, $volname) = @_;
    if ($volname =~ m/^((base-(\d+)-\S+)\/)?((?:(base)|(vm))-(\d+)-\S+)$/)
    {
        # ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format)
        return ('images', $4, $7, $2, $3, $5, 'raw');
    }
    die "unable to parse vitastor volume name '$volname'\n";
}

sub _qemu_option
{
    my ($k, $v) = @_;
    if (defined $v && $v ne "")
    {
        $v =~ s/:/\\:/gso;
        return ":$k=$v";
    }
    return "";
}

sub path
{
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;
    if ($scfg->{vitastor_nbd})
    {
        my $mapped = run_cli($scfg, [ 'ls' ], binary => '/usr/bin/vitastor-nbd');
        my ($kerneldev) = grep { $mapped->{$_}->{image} eq $prefix.$name } keys %$mapped;
        die "Image not mapped via NBD" if !$kerneldev;
        return ($kerneldev, $vmid, $vtype);
    }
    my $path = "vitastor";
    $path .= _qemu_option('config_path', $scfg->{vitastor_config_path});
    # FIXME This is the only exception: etcd_address -> etcd_host for qemu
    $path .= _qemu_option('etcd_host', $scfg->{vitastor_etcd_address});
    $path .= _qemu_option('etcd_prefix', $scfg->{vitastor_etcd_prefix});
    $path .= _qemu_option('image', $prefix.$name);
    return ($path, $vmid, $vtype);
}

sub _find_free_diskname
{
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;
    my $list = _process_list($scfg, $storeid, run_cli($scfg, [ 'ls' ]));
    $list = [ map { $_->{name} } @$list ];
    return PVE::Storage::Plugin::get_next_vm_diskname($list, $storeid, $vmid, undef, $scfg);
}

# Used only in "Create Template" and, in fact, converts a VM into a template
# As a consequence, this is always invoked with the VM powered off
# So we just rename vm-xxx to base-xxx and make it a readonly base layer
sub create_base
{
    my ($class, $storeid, $scfg, $volname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) = $class->parse_volname($volname);
    die "create_base not possible with base image\n" if $isBase;

    my $info = _process_list($scfg, $storeid, run_cli($scfg, [ 'ls', $prefix.$name ]))->[0];
    die "image $name does not exist\n" if !$info;

    die "volname '$volname' contains wrong information about parent {$info->{parent}} $basename\n"
        if $basename && (!$info->{parent} || $info->{parent} ne $basename);

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basename/$newname" : "$newname";
    run_cli($scfg, [ 'modify', '--rename', $prefix.$newname, '--readonly', $prefix.$name ], json => 0);

    return $newvolname;
}

sub clone_image
{
    my ($class, $scfg, $storeid, $volname, $vmid, $snapname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';

    my $snap = '';
    $snap = '@'.$snapname if length $snapname;

    my ($vtype, $basename, $basevmid, undef, undef, $isBase) = $class->parse_volname($volname);
    die "$volname is not a base image and snapname is not provided\n" if !$isBase && !length($snapname);

    my $name = $class->find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename snapname $snap to $name\n";

    my $newvol = "$basename/$name";
    $newvol = $name if length($snapname);

    run_cli($scfg, [ 'create', '--parent', $prefix.$basename.$snap, $prefix.$name ], json => 0);

    return $newvol;
}

sub alloc_image
{
    # $size is in kb in this method
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    die "illegal name '$name' - should be 'vm-$vmid-*'\n" if $name && $name !~ m/^vm-$vmid-/;
    $name = $class->find_free_diskname($storeid, $scfg, $vmid) if !$name;
    run_cli($scfg, [ 'create', '--size', (int(($size+3)/4)*4).'k', '--pool', $scfg->{vitastor_pool}, $prefix.$name ], json => 0);
    return $name;
}

sub free_image
{
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid, undef, undef, undef) = $class->parse_volname($volname);
    $class->deactivate_volume($storeid, $scfg, $volname);
    my $full_list = run_cli($scfg, [ 'ls', '-l' ]);
    my $list = _process_list($scfg, $storeid, $full_list);
    # Remove image and all its snapshots
    my $rm_names = {
        map { ($prefix.$_->{name} => 1) }
        grep { $_->{name} eq $name || substr($_->{name}, 0, length($name)+1) eq ($name.'@') }
        @$list
    };
    my $children = [ grep { $_->{parent_name} && $rm_names->{$_->{parent_name}} } @$full_list ];
    die "Image has children: ".join(', ', map {
        substr($_->{name}, 0, length $prefix) eq $prefix
            ? substr($_->name, length $prefix)
            : $_->{name}
    } @$children)."\n" if @$children;
    my $to_remove = [ grep { $rm_names->{$_->{name}} } @$full_list ];
    for my $rmi (@$to_remove)
    {
        run_cli($scfg, [ 'rm-data', '--pool', $rmi->{pool_id}, '--inode', $rmi->{inode_num} ], json => 0);
    }
    for my $rmi (@$to_remove)
    {
        run_cli($scfg, [ 'rm', $rmi->{name} ], json => 0);
    }
    return undef;
}

sub _process_list
{
    my ($scfg, $storeid, $result) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my $list = [];
    foreach my $el (@$result)
    {
        next if !$el->{name} || length($prefix) && substr($el->{name}, 0, length $prefix) ne $prefix;
        my $name = substr($el->{name}, length $prefix);
        next if $name =~ /@/;
        my ($owner) = $name =~ /^(?:vm|base)-(\d+)-/s;
        next if !defined $owner;
        my $parent = !defined $el->{parent_name}
            ? undef
            : ($prefix eq '' || substr($el->{parent_name}, 0, length $prefix) eq $prefix
                ? substr($el->{parent_name}, length $prefix) : '');
        my $volid = $parent && $parent =~ /^(base-\d+-\S+)$/s
            ? "$storeid:$1/$name" : "$storeid:$name";
        push @$list, {
            format => 'raw',
            volid => $volid,
            name => $name,
            size => $el->{size},
            parent => $parent,
            vmid => $owner,
        };
    }
    return $list;
}

sub list_images
{
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;
    my $list = _process_list($scfg, $storeid, run_cli($scfg, [ 'ls', '-l' ]));
    if ($vollist)
    {
        my $h = { map { ($_ => 1) } @$vollist };
        $list = [ grep { $h->{$_->{volid}} } @$list ]
    }
    elsif (defined $vmid)
    {
        $list = [ grep { $_->{vmid} eq $vmid } @$list ];
    }
    return $list;
}

sub status
{
    my ($class, $storeid, $scfg, $cache) = @_;
    my $stats = [ grep { $_->{name} eq $scfg->{vitastor_pool} } @{ run_cli($scfg, [ 'df' ]) } ]->[0];
    my $free = $stats ? $stats->{max_available} : 0;
    my $used = $stats ? $stats->{used_raw}/($stats->{raw_to_usable}||1) : 0;
    my $total = $free+$used;
    my $active = $stats ? 1 : 0;
    return ($total, $free, $used, $active);
}

sub activate_storage
{
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub deactivate_storage
{
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub map_volume
{
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';

    my ($vtype, $img_name, $vmid) = $class->parse_volname($volname);
    my $name = $img_name;
    $name .= '@'.$snapname if $snapname;

    my $mapped = run_cli($scfg, [ 'ls' ], binary => '/usr/bin/vitastor-nbd');
    my ($kerneldev) = grep { $mapped->{$_}->{image} eq $prefix.$name } keys %$mapped;
    return $kerneldev if $kerneldev && -b $kerneldev; # already mapped

    $kerneldev = run_cli($scfg, [ 'map', '--image', $prefix.$name ], binary => '/usr/bin/vitastor-nbd', json => 0);
    return $kerneldev;
}

sub unmap_volume
{
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;

    my $mapped = run_cli($scfg, [ 'ls' ], binary => '/usr/bin/vitastor-nbd');
    my ($kerneldev) = grep { $mapped->{$_}->{image} eq $prefix.$name } keys %$mapped;
    if ($kerneldev && -b $kerneldev)
    {
        run_cli($scfg, [ 'unmap', $kerneldev ], binary => '/usr/bin/vitastor-nbd', json => 0);
    }

    return 1;
}

sub activate_volume
{
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
    $class->map_volume($storeid, $scfg, $volname, $snapname) if $scfg->{vitastor_nbd};
    return 1;
}

sub deactivate_volume
{
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
    $class->unmap_volume($storeid, $scfg, $volname, $snapname) if $scfg->{vitastor_nbd};
    return 1;
}

sub volume_size_info
{
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my $info = _process_list($scfg, $storeid, run_cli($scfg, [ 'ls', $prefix.$name ]))->[0];
    #return wantarray ? ($size, $format, $used, $parent, $st->ctime) : $size;
    return $info->{size};
}

sub volume_resize
{
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    # $size is in bytes in this method
    run_cli($scfg, [ 'modify', '--resize', (int(($size+4095)/4096)*4).'k', $prefix.$name ], json => 0);
    return undef;
}

sub volume_snapshot
{
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    run_cli($scfg, [ 'create', '--snapshot', $snap, $prefix.$name ], json => 0);
    return undef;
}

sub volume_snapshot_rollback
{
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    run_cli($scfg, [ 'rm', $prefix.$name ], json => 0);
    run_cli($scfg, [ 'create', '--parent', $prefix.$name.'@'.$snap, $prefix.$name ], json => 0);
    return undef;
}

sub volume_snapshot_delete
{
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    run_cli($scfg, [ 'rm', $prefix.$name.'@'.$snap ], json => 0);
    return undef;
}

sub volume_snapshot_needs_fsfreeze
{
    return 1;
}

sub volume_has_feature
{
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;
    my $features = {
        snapshot => { current => 1, snap => 1 },
        clone => { base => 1, snap => 1 },
        template => { current => 1 },
        copy => { base => 1, current => 1, snap => 1 },
        sparseinit => { base => 1, current => 1 },
        rename => { current => 1 },
    };
    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) = $class->parse_volname($volname);
    my $key = undef;
    if ($snapname)
    {
        $key = 'snap';
    }
    else
    {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};
    return undef;
}

sub rename_volume
{
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;
    my $prefix = defined $scfg->{vitastor_prefix} ? $scfg->{vitastor_prefix} : 'pve/';
    my (undef, $source_image, $source_vmid, $base_name, $base_vmid, undef, $format) =
        $class->parse_volname($source_volname);
    $target_volname = $class->find_free_diskname($storeid, $scfg, $target_vmid, $format) if !$target_volname;
    run_cli($scfg, [ 'modify', '--rename', $prefix.$target_volname, $prefix.$source_image ], json => 0);
    $base_name = $base_name ? "${base_name}/" : '';
    return "${storeid}:${base_name}${target_volname}";
}

1;
