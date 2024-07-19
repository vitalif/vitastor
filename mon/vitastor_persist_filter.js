// AntiEtcd persistence filter for Vitastor
// (c) Vitaliy Filippov, 2024
// License: Mozilla Public License 2.0 or Vitastor Network Public License 1.1

function vitastor_persist_filter(cfg)
{
    const prefix = cfg.vitastor_prefix || '/vitastor';
    return (key, value) =>
    {
        if (key.substr(0, prefix.length+'/osd/stats/'.length) == prefix+'/osd/stats/')
        {
            if (value)
            {
                try
                {
                    value = JSON.parse(value);
                    value = JSON.stringify({
                        bitmap_granularity: value.bitmap_granularity || undefined,
                        data_block_size: value.data_block_size || undefined,
                        host: value.host || undefined,
                        immediate_commit: value.immediate_commit || undefined,
                    });
                }
                catch (e)
                {
                    console.error('invalid JSON in '+key+' = '+value+': '+e);
                    value = '{}';
                }
            }
            else
            {
                value = undefined;
            }
            return value;
        }
        else if (key.substr(0, prefix.length+'/osd/'.length) == prefix+'/osd/' ||
            key.substr(0, prefix.length+'/inode/stats/'.length) == prefix+'/inode/stats/' ||
            key.substr(0, prefix.length+'/pg/stats/'.length) == prefix+'/pg/stats/' || // old name
            key.substr(0, prefix.length+'/pgstats/'.length) == prefix+'/pgstats/' ||
            key.substr(0, prefix.length+'/pool/stats/'.length) == prefix+'/pool/stats/' ||
            key == prefix+'/stats')
        {
            return undefined;
        }
        return value;
    };
}

module.exports = vitastor_persist_filter;
