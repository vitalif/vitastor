// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

module.exports = {
    scale_pg_count,
};

function scale_pg_count(prev_pgs, prev_pg_history, new_pg_history, new_pg_count)
{
    const old_pg_count = prev_pgs.length;
    // Add all possibly intersecting PGs into the history of new PGs
    if (!(new_pg_count % old_pg_count))
    {
        // New PG count is a multiple of the old PG count
        const mul = (new_pg_count / old_pg_count);
        for (let i = 0; i < new_pg_count; i++)
        {
            const old_i = Math.floor(new_pg_count / mul);
            new_pg_history[i] = JSON.parse(JSON.stringify(prev_pg_history[1+old_i]));
        }
    }
    else if (!(old_pg_count % new_pg_count))
    {
        // Old PG count is a multiple of the new PG count
        const mul = (old_pg_count / new_pg_count);
        for (let i = 0; i < new_pg_count; i++)
        {
            new_pg_history[i] = {
                osd_sets: [],
                all_peers: [],
                epoch: 0,
            };
            for (let j = 0; j < mul; j++)
            {
                new_pg_history[i].osd_sets.push(prev_pgs[i*mul]);
                const hist = prev_pg_history[1+i*mul+j];
                if (hist && hist.osd_sets && hist.osd_sets.length)
                {
                    Array.prototype.push.apply(new_pg_history[i].osd_sets, hist.osd_sets);
                }
                if (hist && hist.all_peers && hist.all_peers.length)
                {
                    Array.prototype.push.apply(new_pg_history[i].all_peers, hist.all_peers);
                }
                if (hist && hist.epoch)
                {
                    new_pg_history[i].epoch = new_pg_history[i].epoch < hist.epoch ? hist.epoch : new_pg_history[i].epoch;
                }
            }
        }
    }
    else
    {
        // Any PG may intersect with any PG after non-multiple PG count change
        // So, merge ALL PGs history
        let all_sets = {};
        let all_peers = {};
        let max_epoch = 0;
        for (const pg of prev_pgs)
        {
            all_sets[pg.join(' ')] = pg;
        }
        for (const pg in prev_pg_history)
        {
            const hist = prev_pg_history[pg];
            if (hist && hist.osd_sets)
            {
                for (const pg of hist.osd_sets)
                {
                    all_sets[pg.join(' ')] = pg;
                }
            }
            if (hist && hist.all_peers)
            {
                for (const osd_num of hist.all_peers)
                {
                    all_peers[osd_num] = Number(osd_num);
                }
            }
            if (hist && hist.epoch)
            {
                max_epoch = max_epoch < hist.epoch ? hist.epoch : max_epoch;
            }
        }
        all_sets = Object.values(all_sets);
        all_peers = Object.values(all_peers);
        for (let i = 0; i < new_pg_count; i++)
        {
            new_pg_history[i] = { osd_sets: all_sets, all_peers, epoch: max_epoch };
        }
    }
    // Mark history keys for removed PGs as removed
    for (let i = new_pg_count; i < old_pg_count; i++)
    {
        new_pg_history[i] = null;
    }
    if (old_pg_count < new_pg_count)
    {
        for (let i = new_pg_count-1; i >= 0; i--)
        {
            prev_pgs[i] = prev_pgs[Math.floor(i/new_pg_count*old_pg_count)];
        }
    }
    else if (old_pg_count > new_pg_count)
    {
        for (let i = 0; i < new_pg_count; i++)
        {
            prev_pgs[i] = prev_pgs[Math.round(i/new_pg_count*old_pg_count)];
        }
        prev_pgs.splice(new_pg_count, old_pg_count-new_pg_count);
    }
}
