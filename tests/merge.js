const fsp = require('fs').promises;

async function merge(file1, file2, out)
{
    if (!out)
    {
        console.error('USAGE: nodejs merge.js layer1 layer2 output');
        process.exit();
    }
    const layer1 = await fsp.readFile(file1);
    const layer2 = await fsp.readFile(file2);
    const zero = Buffer.alloc(4096);
    for (let i = 0; i < layer2.length; i += 4096)
    {
        if (zero.compare(layer2, i, i+4096) != 0)
        {
            layer2.copy(layer1, i, i, i+4096);
        }
    }
    await fsp.writeFile(out, layer1);
}

merge(process.argv[2], process.argv[3], process.argv[4]);
