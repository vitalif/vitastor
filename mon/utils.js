// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const os = require('os');

function local_ips(all)
{
    const ips = [];
    const ifaces = os.networkInterfaces();
    for (const ifname in ifaces)
    {
        for (const iface of ifaces[ifname])
        {
            if (iface.family == 'IPv4' && !iface.internal || all)
            {
                ips.push(iface.address);
            }
        }
    }
    return ips;
}

function b64(str)
{
    return Buffer.from(str).toString('base64');
}

function de64(str)
{
    return Buffer.from(str, 'base64').toString();
}

module.exports = {
    b64,
    de64,
    local_ips,
};
