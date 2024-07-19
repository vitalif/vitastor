// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const fsp = require('fs').promises;
const http = require('http');
const https = require('https');

async function create_http_server(cfg, handler)
{
    let server;
    if (cfg.mon_https_cert)
    {
        const tls = {
            key: await fsp.readFile(cfg.mon_https_key),
            cert: await fsp.readFile(cfg.mon_https_cert),
        };
        if (cfg.mon_https_ca)
        {
            tls.mon_https_ca = await fsp.readFile(cfg.mon_https_ca);
        }
        if (cfg.mon_https_client_auth)
        {
            tls.requestCert = true;
        }
        server = https.createServer(tls, handler);
    }
    else
    {
        server = http.createServer(handler);
    }
    try
    {
        let err;
        server.once('error', e => err = e);
        server.listen(cfg.mon_http_port || 8060, cfg.mon_http_ip || undefined);
        if (err)
            throw err;
    }
    catch (e)
    {
        console.error(
            'HTTP server disabled because listen at address: '+
            (cfg.mon_http_ip || '')+':'+(cfg.mon_http_port || 9090)+' failed with error: '+e
        );
        return null;
    }
    return server;
}

module.exports = { create_http_server };
