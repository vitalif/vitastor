// Copyright (c) Vitaliy Filippov, 2019+
// License: MIT

// Simple tool to calculate journal and metadata offsets for a single device
// Will be replaced by smarter tools in the future

const fs = require('fs').promises;
const child_process = require('child_process');

async function run()
{
    const options = {
        object_size: 128*1024,
        bitmap_granularity: 4096,
        journal_size: 16*1024*1024,
        device_block_size: 4096,
        journal_offset: 0,
        device_size: 0,
        format: 'text',
    };
    for (let i = 2; i < process.argv.length; i++)
    {
        if (process.argv[i].substr(0, 2) == '--')
        {
            options[process.argv[i].substr(2)] = process.argv[i+1];
            i++;
        }
    }
    if (!options.device)
    {
        process.stderr.write('USAGE: nodejs '+process.argv[1]+' --device /dev/sdXXX\n');
        process.exit(1);
    }
    options.device_size = Number(options.device_size);
    let device_size = options.device_size;
    if (!device_size)
    {
        const st = await fs.stat(options.device);
        options.device_block_size = st.blksize;
        if (st.isBlockDevice())
            device_size = Number(await system("/sbin/blockdev --getsize64 "+options.device));
        else
            device_size = st.size;
    }
    if (!device_size)
    {
        process.stderr.write('Failed to get device size\n');
        process.exit(1);
    }
    options.journal_offset = Math.ceil(options.journal_offset/options.device_block_size)*options.device_block_size;
    const meta_offset = options.journal_offset + Math.ceil(options.journal_size/options.device_block_size)*options.device_block_size;
    const meta_entry_size = 24 + 2*options.object_size/options.bitmap_granularity/8;
    const entries_per_block = Math.floor(options.device_block_size / meta_entry_size);
    const object_count = Math.floor((device_size-meta_offset)/options.object_size);
    const meta_size = Math.ceil(1 + object_count / entries_per_block) * options.device_block_size;
    const data_offset = meta_offset + meta_size;
    const meta_size_fmt = (meta_size > 1024*1024*1024 ? Math.round(meta_size/1024/1024/1024*100)/100+" GB"
        : Math.round(meta_size/1024/1024*100)/100+" MB");
    if (options.format == 'text' || options.format == 'options')
    {
        if (options.format == 'text')
        {
            process.stderr.write(
                `Metadata size: ${meta_size_fmt}\n`+
                `Options for the OSD:\n`
            );
        }
        process.stdout.write(
            (options.device_block_size != 4096 ?
                `    --meta_block_size ${options.device}\n`+
                `    --journal_block-size ${options.device}\n` : '')+
            `    --data_device ${options.device}\n`+
            `    --journal_offset ${options.journal_offset}\n`+
            `    --meta_offset ${meta_offset}\n`+
            `    --data_offset ${data_offset}\n`+
            (options.device_size ? `    --data_size ${device_size-data_offset}\n` : '')
        );
    }
    else if (options.format == 'env')
    {
        process.stdout.write(
            `journal_offset=${options.journal_offset}\n`+
            `meta_offset=${meta_offset}\n`+
            `data_offset=${data_offset}\n`+
            `data_size=${device_size-data_offset}\n`
        );
    }
    else
        process.stdout.write('Unknown format: '+options.format);
}

function system(cmd)
{
    return new Promise((ok, no) => child_process.exec(cmd, { maxBuffer: 64*1024*1024 }, (err, stdout/*, stderr*/) => (err ? no(err.message) : ok(stdout))));
}

run().catch(err => { console.error(err); process.exit(1); });
