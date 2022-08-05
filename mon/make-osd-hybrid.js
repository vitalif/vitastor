#!/usr/bin/nodejs
// systemd unit generator for hybrid (HDD+SSD) vitastor OSDs
// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1

// USAGE: nodejs make-osd-hybrid.js [--disable_ssd_cache 0] [--disable_hdd_cache 0] /dev/sda /dev/sdb /dev/sdc /dev/sdd ...
// I.e. - just pass all HDDs and SSDs mixed, the script will decide where
// to put journals on its own

const fs = require('fs');
const fsp = fs.promises;
const child_process = require('child_process');

const options = {
    debug: 1,
    journal_size: 1024*1024*1024,
    min_meta_size: 1024*1024*1024,
    object_size: 1024*1024,
    bitmap_granularity: 4096,
    device_block_size: 4096,
    disable_ssd_cache: 1,
    disable_hdd_cache: 1,
};

run().catch(console.fatal);

async function run()
{
    const device_list = parse_options();
    await system_or_die("mkdir -p /var/log/vitastor; chown vitastor /var/log/vitastor");
    // Collect devices
    const all_devices = await collect_devices(device_list);
    const ssds = all_devices.filter(d => d.ssd);
    const hdds = all_devices.filter(d => !d.ssd);
    // Collect existing OSD units
    const osd_units = await collect_osd_units();
    // Count assigned HDD journals and unallocated space for each SSD
    await check_journal_count(ssds, osd_units);
    // Create new OSDs
    await create_new_hybrid_osds(hdds, ssds, osd_units);
    process.exit(0);
}

function parse_options()
{
    const devices = [];
    const opt = {};
    for (let i = 2; i < process.argv.length; i++)
    {
        const arg = process.argv[i];
        if (arg == '--help' || arg == '-h')
        {
            opt.help = true;
            break;
        }
        else if (arg.substr(0, 2) == '--')
            opt[arg.substr(2)] = process.argv[++i];
        else
            devices.push(arg);
    }
    if (opt.help || !devices.length)
    {
        console.log(
            'Prepare hybrid (HDD+SSD) Vitastor OSDs\n'+
            '(c) Vitaliy Filippov, 2019+, license: VNPL-1.1\n\n'+
            'USAGE: nodejs make-osd-hybrid.js [OPTIONS] /dev/sda /dev/sdb /dev/sdc ...\n'+
            'Just pass all your SSDs and HDDs in any order, the script will distribute OSDs for you.\n\n'+
            'OPTIONS (with defaults):\n'+
            Object.keys(options).map(k => `  --${k} ${options[k]}`).join('\n')
        );
        process.exit(0);
    }
    for (const k in opt)
        options[k] = opt[k];
    return devices;
}

// Collect devices
async function collect_devices(devices_to_check)
{
    const devices = [];
    for (const dev of devices_to_check)
    {
        if (dev.substr(0, 5) != '/dev/')
        {
            console.log(`${dev} does not start with /dev/, skipping`);
            continue;
        }
        if (!await file_exists('/sys/block/'+dev.substr(5)))
        {
            console.log(`${dev} is a partition, skipping`);
            continue;
        }
        // Check if the device is an SSD
        const rot = '/sys/block/'+dev.substr(5)+'/queue/rotational';
        if (!await file_exists(rot))
        {
            console.log(`${dev} does not have ${rot} to check whether it's an SSD, skipping`);
            continue;
        }
        const ssd = !parseInt(await fsp.readFile(rot, { encoding: 'utf-8' }));
        // Check if the device has partition table
        let [ has_partition_table, parts ] = await system(`sfdisk --dump ${dev} --json`);
        if (has_partition_table != 0)
        {
            // Check if the device has any data
            const [ has_data, out ] = await system(`blkid -p ${dev}`);
            if (has_data == 0)
            {
                console.log(`${dev} contains data, skipping:\n  ${out.trim().replace(/\n/g, '\n  ')}`);
                continue;
            }
        }
        parts = parts ? JSON.parse(parts).partitiontable : null;
        if (parts && parts.label != 'gpt')
        {
            console.log(`${dev} contains "${parts.label}" partition table, only GPT is supported, skipping`);
            continue;
        }
        devices.push({
            path: dev,
            ssd,
            parts,
        });
    }
    return devices;
}

// Collect existing OSD units
async function collect_osd_units()
{
    const units = [];
    for (const unit of (await system("ls /etc/systemd/system/vitastor-osd*.service"))[1].trim().split('\n'))
    {
        if (!unit)
        {
            continue;
        }
        let cmd = /^ExecStart\s*=\s*(([^\n]*\\\n)*[^\n]*)/.exec(await fsp.readFile(unit, { encoding: 'utf-8' }));
        if (!cmd)
        {
            console.log('ExecStart= not found in '+unit+', skipping')
            continue;
        }
        let kv = {}, key;
        cmd = cmd[1].replace(/^bash\s+-c\s+'/, '')
            .replace(/>>\s*\S+2>\s*&1\s*'$/, '')
            .replace(/\s*\\\n\s*/g, ' ')
            .replace(/([^\s']+)|'([^']+)'/g, (m, m1, m2) =>
            {
                m1 = m1||m2;
                if (key == null)
                {
                    if (m1.substr(0, 2) != '--')
                    {
                        console.log('Strange command line in '+unit+', stopping');
                        process.exit(1);
                    }
                    key = m1.substr(2);
                }
                else
                {
                    kv[key] = m1;
                    key = null;
                }
            });
        units.push(kv);
    }
    return units;
}

// Count assigned HDD journals and unallocated space for each SSD
async function check_journal_count(ssds, osd_units)
{
    const units_by_journal = osd_units.reduce((a, c) =>
    {
        if (c.journal_device)
            a[c.journal_device] = c;
        return a;
    }, {});
    for (const dev of ssds)
    {
        dev.journals = 0;
        if (dev.parts)
        {
            for (const part of dev.parts.partitions)
            {
                if (part.uuid && units_by_journal['/dev/disk/by-partuuid/'+part.uuid.toLowerCase()])
                {
                    dev.journals++;
                }
            }
            dev.free = free_from_parttable(dev.parts);
        }
        else
        {
            dev.free = parseInt(await system_or_die("blockdev --getsize64 "+dev.path));
        }
    }
}

async function create_new_hybrid_osds(hdds, ssds, osd_units)
{
    const units_by_disk = osd_units.reduce((a, c) => { a[c.data_device] = c; return a; }, {});
    for (const dev of hdds)
    {
        if (!dev.parts)
        {
            // HDD is not partitioned yet, create a single partition
            // + is the "default value" for sfdisk
            await system_or_die('sfdisk '+dev.path, 'label: gpt\n\n+ +\n');
            dev.parts = JSON.parse(await system_or_die('sfdisk --dump '+dev.path+' --json')).partitiontable;
        }
        if (dev.parts.partitions.length != 1)
        {
            console.log(dev.path+' has more than 1 partition, skipping');
        }
        else if ((dev.parts.partitions[0].start + dev.parts.partitions[0].size) != (1 + dev.parts.lastlba))
        {
            console.log(dev.path+'1 is not a whole-disk partition, skipping');
        }
        else if (!dev.parts.partitions[0].uuid)
        {
            console.log(dev.parts.partitions[0].node+' does not have UUID. Please repartition '+dev.path+' with GPT');
        }
        else if (!units_by_disk['/dev/disk/by-partuuid/'+dev.parts.partitions[0].uuid.toLowerCase()])
        {
            await create_hybrid_osd(dev, ssds);
        }
    }
}

async function create_hybrid_osd(dev, ssds)
{
    // Create a new OSD
    // Calculate metadata size
    const data_device = '/dev/disk/by-partuuid/'+dev.parts.partitions[0].uuid.toLowerCase();
    const data_size = dev.parts.partitions[0].size * dev.parts.sectorsize;
    const meta_entry_size = 24 + 2*options.object_size/options.bitmap_granularity/8;
    const entries_per_block = Math.floor(options.device_block_size / meta_entry_size);
    const object_count = Math.floor(data_size / options.object_size);
    let meta_size = Math.ceil(1 + object_count / entries_per_block) * options.device_block_size;
    // Leave some extra space for future metadata formats and round metadata area size to multiples of 1 MB
    meta_size = 2*meta_size;
    meta_size = Math.ceil(meta_size/1024/1024) * 1024*1024;
    if (meta_size < options.min_meta_size)
        meta_size = options.min_meta_size;
    let journal_size = Math.ceil(options.journal_size/1024/1024) * 1024*1024;
    // Pick an SSD for journal, balancing the number of journals across SSDs
    let selected_ssd;
    for (const ssd of ssds)
        if (ssd.free >= (meta_size+journal_size) && (!selected_ssd || selected_ssd.journals > ssd.journals))
            selected_ssd = ssd;
    if (!selected_ssd)
    {
        console.error('Could not find free space for SSD journal and metadata for '+dev.path);
        process.exit(1);
    }
    // Allocate an OSD number
    const osd_num = (await system_or_die("vitastor-cli alloc-osd")).trim();
    if (!osd_num)
    {
        console.error('Failed to run vitastor-cli alloc-osd');
        process.exit(1);
    }
    console.log('Creating OSD '+osd_num+' on '+dev.path+' (HDD) with journal and metadata on '+selected_ssd.path+' (SSD)');
    // Add two partitions: journal and metadata
    const new_parts = await add_partitions(selected_ssd, [ journal_size, meta_size ]);
    selected_ssd.journals++;
    const journal_device = '/dev/disk/by-partuuid/'+new_parts[0].uuid.toLowerCase();
    const meta_device = '/dev/disk/by-partuuid/'+new_parts[1].uuid.toLowerCase();
    // Wait until the device symlinks appear
    while (!await file_exists(journal_device))
    {
        await new Promise(ok => setTimeout(ok, 100));
    }
    while (!await file_exists(meta_device))
    {
        await new Promise(ok => setTimeout(ok, 100));
    }
    // Zero out metadata and journal
    await system_or_die("dd if=/dev/zero of="+journal_device+" bs=1M count="+(journal_size/1024/1024)+" oflag=direct");
    await system_or_die("dd if=/dev/zero of="+meta_device+" bs=1M count="+(meta_size/1024/1024)+" oflag=direct");
    // Create unit file for the OSD
    const has_scsi_cache_type = options.disable_ssd_cache &&
        (await system("ls /sys/block/"+selected_ssd.path.substr(5)+"/device/scsi_disk/*/cache_type"))[0] == 0;
    const write_through = options.disable_ssd_cache && (
        has_scsi_cache_type || selected_ssd.path.substr(5, 4) == 'nvme'
        && (await system_or_die("/sys/block/"+selected_ssd.path.substr(5)+"/queue/write_cache")).trim() == "write through");
    await fsp.writeFile('/etc/systemd/system/vitastor-osd'+osd_num+'.service',
`[Unit]
Description=Vitastor object storage daemon osd.${osd_num}
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target
PartOf=vitastor.target

[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
LimitMEMLOCK=infinity
ExecStart=bash -c '/usr/bin/vitastor-osd \\
    --osd_num ${osd_num} ${write_through
        ? "--disable_meta_fsync 1 --disable_journal_fsync 1 --immediate_commit "+(options.disable_hdd_cache ? "all" : "small")
        : ""} \\
    --throttle_small_writes 1 \\
    --disk_alignment ${options.device_block_size} \\
    --journal_block_size ${options.device_block_size} \\
    --meta_block_size ${options.device_block_size} \\
    --journal_no_same_sector_overwrites true \\
    --journal_sector_buffer_count 1024 \\
    --block_size ${options.object_size} \\
    --data_device ${data_device} \\
    --journal_device ${journal_device} \\
    --meta_device ${meta_device} >>/var/log/vitastor/osd${osd_num}.log 2>&1'
WorkingDirectory=/
ExecStartPre=+chown vitastor:vitastor ${data_device}
ExecStartPre=+chown vitastor:vitastor ${journal_device}
ExecStartPre=+chown vitastor:vitastor ${meta_device}${
    has_scsi_cache_type
    ? "\nExecStartPre=+bash -c 'D=$$$(readlink "+journal_device+"); echo write through > $$$(dirname /sys/block/*/$$\${D##*/})/device/scsi_disk/*/cache_type'"
    : ""}${
    options.disable_hdd_cache
    ? "\nExecStartPre=+bash -c 'D=$$$(readlink "+data_device+"); echo write through > $$$(dirname /sys/block/*/$$\${D##*/})/device/scsi_disk/*/cache_type'"
    : ""}
User=vitastor
PrivateTmp=false
TasksMax=infinity
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=vitastor.target
`);
    await system_or_die("systemctl enable vitastor-osd"+osd_num);
}

async function add_partitions(dev, sizes)
{
    let script = 'label: gpt\n\n';
    if (dev.parts)
    {
        // Old partitions
        for (const part of dev.parts.partitions)
        {
            script += part.node+': '+Object.keys(part).map(k => k == 'node' ? '' : k+'='+part[k]).filter(k => k).join(', ')+'\n';
        }
    }
    // New partitions
    for (const size of sizes)
    {
        script += '+ '+Math.ceil(size/1024)+'KiB\n';
    }
    await system_or_die('sfdisk '+dev.path, script);
    // Get new partition table and find the new partition
    const newpt = JSON.parse(await system_or_die('sfdisk --dump '+dev.path+' --json')).partitiontable;
    const old_nodes = dev.parts ? dev.parts.partitions.reduce((a, c) => { a[c.uuid] = true; return a; }, {}) : {};
    const new_nodes = newpt.partitions.filter(part => !old_nodes[part.uuid]);
    if (new_nodes.length != sizes.length)
    {
        console.error('Failed to partition '+dev.path+': new partitions not found in table');
        process.exit(1);
    }
    dev.parts = newpt;
    dev.free = free_from_parttable(newpt);
    return new_nodes;
}

function free_from_parttable(pt)
{
    let free = pt.lastlba + 1 - pt.firstlba;
    for (const part of pt.partitions)
    {
        free -= part.size;
    }
    free *= pt.sectorsize;
    return free;
}

async function system_or_die(cmd, input = '')
{
    let [ exitcode, stdout, stderr ] = await system(cmd, input);
    if (exitcode != 0)
    {
        console.error(cmd+' failed: '+stderr);
        process.exit(1);
    }
    return stdout;
}

async function system(cmd, input = '')
{
    if (options.debug)
    {
        process.stderr.write('+ '+cmd+(input ? " <<EOF\n"+input.replace(/\s*$/, '\n')+"EOF" : '')+'\n');
    }
    const cp = child_process.spawn(cmd, { shell: true });
    let stdout = '', stderr = '', finish_cb;
    cp.stdout.on('data', buf => stdout += buf.toString());
    cp.stderr.on('data', buf => stderr += buf.toString());
    cp.on('exit', () => finish_cb && finish_cb());
    cp.stdin.write(input);
    cp.stdin.end();
    if (cp.exitCode == null)
    {
        await new Promise(ok => finish_cb = ok);
    }
    return [ cp.exitCode, stdout, stderr ];
}

async function file_exists(filename)
{
    return new Promise((ok, no) => fs.access(filename, fs.constants.R_OK, err => ok(!err)));
}
