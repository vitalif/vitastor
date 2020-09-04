#!/usr/bin/node

const Mon = require('./mon.js');

const options = {};

for (let i = 2; i < process.argv.length; i++)
{
    if (process.argv[i].substr(0, 2) == '--')
    {
        options[process.argv[i].substr(2)] = process.argv[i+1];
        i++;
    }
}

if (!options.etcd_url)
{
    console.error('USAGE: '+process.argv[0]+' '+process.argv[1]+' --etcd_url "http://127.0.0.1:2379,..." --etcd_prefix "/vitastor" --etcd_start_timeout 5');
    process.exit();
}

new Mon(options).start();
