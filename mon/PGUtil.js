// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

module.exports = {
    scale_pg_count,
};

function add_pg_history(new_pg_history, new_pg, prev_pgs, prev_pg_history, old_pg)
{
    if (!new_pg_history[new_pg])
    {
        new_pg_history[new_pg] = {
            osd_sets: {},
            all_peers: {},
            epoch: 0,
        };
    }
    const nh = new_pg_history[new_pg], oh = prev_pg_history[old_pg];
    nh.osd_sets[prev_pgs[old_pg].join(' ')] = prev_pgs[old_pg];
    if (oh && oh.osd_sets && oh.osd_sets.length)
    {
        for (const pg of oh.osd_sets)
        {
            nh.osd_sets[pg.join(' ')] = pg.map(osd_num => Number(osd_num));
        }
    }
    if (oh && oh.all_peers && oh.all_peers.length)
    {
        for (const osd_num of oh.all_peers)
        {
            nh.all_peers[osd_num] = Number(osd_num);
        }
    }
    if (oh && oh.epoch)
    {
        nh.epoch = nh.epoch < oh.epoch ? oh.epoch : nh.epoch;
    }
}

function finish_pg_history(merged_history)
{
    merged_history.osd_sets = Object.values(merged_history.osd_sets);
    merged_history.all_peers = Object.values(merged_history.all_peers);
}

function scale_pg_count(prev_pgs, prev_pg_history, new_pg_history, new_pg_count)
{
    const old_pg_count = prev_pgs.length;
    // Add all possibly intersecting PGs to the history of new PGs
    if (!(new_pg_count % old_pg_count))
    {
        // New PG count is a multiple of old PG count
        for (let i = 0; i < new_pg_count; i++)
        {
            add_pg_history(new_pg_history, i, prev_pgs, prev_pg_history, i % old_pg_count);
            finish_pg_history(new_pg_history[i]);
        }
    }
    else if (!(old_pg_count % new_pg_count))
    {
        // Old PG count is a multiple of the new PG count
        const mul = (old_pg_count / new_pg_count);
        for (let i = 0; i < new_pg_count; i++)
        {
            for (let j = 0; j < mul; j++)
            {
                add_pg_history(new_pg_history, i, prev_pgs, prev_pg_history, i+j*new_pg_count);
            }
            finish_pg_history(new_pg_history[i]);
        }
    }
    else
    {
        // Any PG may intersect with any PG after non-multiple PG count change
        // So, merge ALL PGs history
        let merged_history = {};
        for (let i = 0; i < old_pg_count; i++)
        {
            add_pg_history(merged_history, 1, prev_pgs, prev_pg_history, i);
        }
        finish_pg_history(merged_history[1]);
        for (let i = 0; i < new_pg_count; i++)
        {
            new_pg_history[i] = { ...merged_history[1] };
        }
    }
    // Mark history keys for removed PGs as removed
    for (let i = new_pg_count; i < old_pg_count; i++)
    {
        new_pg_history[i] = null;
    }
    // Just for the lp_solve optimizer - pick a "previous" PG for each "new" one
    if (old_pg_count < new_pg_count)
    {
        for (let i = old_pg_count; i < new_pg_count; i++)
        {
            prev_pgs[i] = prev_pgs[i % old_pg_count];
        }
    }
    else if (old_pg_count > new_pg_count)
    {
        prev_pgs.splice(new_pg_count, old_pg_count-new_pg_count);
    }
}
