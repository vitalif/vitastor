#!/usr/bin/node
// Upgrade tool for OSD units generated with make-osd.sh and make-osd-hybrid.js

const fsp = require('fs').promises;
const child_process = require('child_process');

upgrade_osd(process.argv[2]).catch(e =>
{
    console.error(e.message);
    process.exit(1);
});

async function upgrade_osd(unit)
{
    if (!unit)
        throw new Error('USAGE: '+process.argv[0]+' '+process.argv[1]+' /etc/systemd/system/vitastor-osd<NUMBER>.service');
    let service_name = /^.*\/(vitastor-osd\d+)\.service$/.exec(unit);
    if (!service_name)
        throw new Error(unit+' is not a service named vitastor-osd<number>');
    service_name = service_name[1];
    // Parse the unit
    const text = await fsp.readFile(unit, { encoding: 'utf-8' });
    let cmd = /\nExecStart\s*=[^\n]+vitastor-osd\s*(([^\\\n&>\d]+|\\[ \t\r]*\n|\d[^>])+)/.exec(text);
    if (!cmd)
        throw new Error('Failed to extract ExecStart command from '+unit);
    cmd = cmd[1].replace(/\\[ \t\r]*\n/g, '').split(/\s+/);
    const options = {};
    for (let i = 0; i < cmd.length-1; i += 2)
    {
        if (cmd[i].substr(0, 2) == '--')
            options[cmd[i].substr(2)] = cmd[i+1];
        else
            i--;
    }
    if (!options['osd_num'] || !options['data_device'])
        throw new Error('osd_num or data_device are missing in '+unit);
    if (options['data_device'].substr(0, '/dev/disk/by-partuuid/'.length) != '/dev/disk/by-partuuid/' ||
        options['meta_device'] && options['meta_device'].substr(0, '/dev/disk/by-partuuid/'.length) != '/dev/disk/by-partuuid/' ||
        options['journal_device'] && options['journal_device'].substr(0, '/dev/disk/by-partuuid/'.length) != '/dev/disk/by-partuuid/')
    {
        throw new Error(
            'data_device, meta_device and journal_device must begin with'+
            ' /dev/disk/by-partuuid/ i.e. they must be GPT partitions identified by UUIDs'
        );
    }
    // Stop and disable the service
    await system_or_die("systemctl disable --now "+service_name);
    const j_o = BigInt(options['journal_offset'] || 0);
    const m_o = BigInt(options['meta_offset'] || 0);
    const d_o = BigInt(options['data_offset'] || 0);
    const m_is_d = !options['meta_device'] || options['meta_device'] == options['data_device'];
    const j_is_m = !options['journal_device'] || options['journal_device'] == options['meta_device'];
    const j_is_d = (options['journal_device'] || options['meta_device'] || options['data_device']) == options['data_device'];
    if (d_o < 4096 || j_o < 4096 || m_o < 4096)
    {
        // Resize data
        let blk = BigInt(options['block_size'] || 128*1024);
        let resize = {};
        if (d_o < 4096 || m_is_d && m_o < 4096 && m_o < d_o || j_is_d && j_o < 4096 && j_o < d_o)
        {
            resize.new_data_offset = d_o+blk;
            if (m_is_d && m_o < d_o)
                resize.new_meta_offset = m_o+blk;
            if (j_is_d && j_o < d_o)
                resize.new_journal_offset = j_o+blk;
        }
        if (!m_is_d && m_o < 4096)
        {
            resize.new_meta_offset = m_o+4096n;
            if (j_is_m && m_o < j_o)
                resize.new_journal_offset = j_o+4096n;
        }
        if (!j_is_d && !j_is_m && j_o < 4096)
            resize.new_journal_offset = j_o+4096n;
        const resize_opts = Object.keys(resize).map(k => ` --${k} ${resize[k]}`).join('');
        const resize_cmd = 'vitastor-disk resize'+
            Object.keys(options).map(k => ` --${k} ${options[k]}`).join('')+resize_opts;
        await system_or_die(resize_cmd, { no_cmd_on_err: true });
        for (let k in resize)
            options[k.substr(4)] = ''+resize[k];
    }
    // Write superblock
    const sb = JSON.stringify(options);
    await system_or_die('vitastor-disk write-sb '+options['data_device'], { input: sb });
    if (!m_is_d)
        await system_or_die('vitastor-disk write-sb '+options['meta_device'], { input: sb });
    if (!j_is_d && !j_is_m)
        await system_or_die('vitastor-disk write-sb '+options['journal_device'], { input: sb });
    // Change partition type
    await fix_partition_type(options['data_device']);
    if (!m_is_d)
        await fix_partition_type(options['meta_device']);
    if (!j_is_d && !j_is_m)
        await fix_partition_type(options['journal_device']);
    // Enable the new unit
    await system_or_die("systemctl enable --now vitastor-osd@"+options['osd_num']);
    console.log('\nOK: Converted OSD '+options['osd_num']+' to the new scheme. The new service name is vitastor-osd@'+options['osd_num']);
}

async function fix_partition_type(dev)
{
    const uuid = dev.replace(/^.*\//, '').toLowerCase();
    const parent_dev = (await fsp.realpath(dev)).replace(/((\d)p|(\D))?\d+$/, '$2$3');
    const pt = JSON.parse(await system_or_die('sfdisk --dump '+parent_dev+' --json', { get_out: true })).partitiontable;
    let script = 'label: gpt\n\n';
    for (const part of pt.partitions)
    {
        if (part.uuid.toLowerCase() == uuid)
            part.type = 'e7009fac-a5a1-4d72-af72-53de13059903';
        script += part.node+': '+Object.keys(part).map(k => k == 'node' ? '' : k+'='+part[k]).filter(k => k).join(', ')+'\n';
    }
    await system_or_die('sfdisk --force '+parent_dev, { input: script, get_out: true });
}

async function system_or_die(cmd, options = {})
{
    let [ exitcode, stdout, stderr ] = await system(cmd, options);
    if (exitcode != 0)
        throw new Error((!options.no_cmd_on_err ? cmd : 'Command')+' failed'+(options.get_err ? ': '+stderr : ''));
    return stdout;
}

async function system(cmd, options = {})
{
    process.stderr.write('Running: '+cmd+(options.input != null ? " <<EOF\n"+options.input.replace(/\s*$/, '\n')+"EOF" : '')+'\n');
    const cp = child_process.spawn(cmd, {
        shell: true,
        stdio: [ 'pipe', options.get_out ? 'pipe' : 1, options.get_err ? 'pipe' : 1 ],
    });
    let stdout = '', stderr = '', finish_cb;
    if (options.get_out)
        cp.stdout.on('data', buf => stdout += buf.toString());
    if (options.get_err)
        cp.stderr.on('data', buf => stderr += buf.toString());
    cp.on('exit', () => finish_cb && finish_cb());
    if (options.input != null)
        cp.stdin.write(options.input);
    cp.stdin.end();
    if (cp.exitCode == null)
        await new Promise(ok => finish_cb = ok);
    return [ cp.exitCode, stdout, stderr ];
}
