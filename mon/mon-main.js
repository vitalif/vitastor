#!/usr/bin/node

// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const Mon = require('./mon.js');

const options = {};

for (let i = 2; i < process.argv.length; i++)
{
    if (process.argv[i] === '-h' || process.argv[i] === '--help')
    {
        console.error('USAGE: '+process.argv[0]+' '+process.argv[1]+' [--verbose 1]'+
            ' [--etcd_address "http://127.0.0.1:2379,..."] [--config_path /etc/vitastor/vitastor.conf]'+
            ' [--etcd_prefix "/vitastor"] [--etcd_start_timeout 5]');
        process.exit();
    }
    else if (process.argv[i].substr(0, 2) == '--')
    {
        options[process.argv[i].substr(2)] = process.argv[i+1];
        i++;
    }
}

new Mon(options).start().catch(e => { console.error(e); process.exit(1); });
