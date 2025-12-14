// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

function get_osd_tree(global_config, state)
{
    const levels = global_config.placement_levels||{};
    levels.host = levels.host || 100;
    levels.osd = levels.osd || 101;
    const tree = {};
    let up_osds = {};
    // This requires monitor system time to be in sync with OSD system times (at least to some extent)
    const down_time = Date.now()/1000 - global_config.osd_out_time;
    for (const osd_num of Object.keys(state.osd.stats).sort((a, b) => a - b))
    {
        const stat = state.osd.stats[osd_num];
        const osd_cfg = state.config.osd[osd_num];
        let reweight = osd_cfg == null ? 1 : Number(osd_cfg.reweight);
        if (isNaN(reweight) || reweight < 0 || reweight > 1)
            reweight = 1;
        if (stat && stat.size && reweight && (state.osd.state[osd_num] || Number(stat.time) >= down_time ||
            osd_cfg && osd_cfg.noout))
        {
            // Numeric IDs are reserved for OSDs
            if (state.osd.state[osd_num] && reweight > 0)
            {
                // React to down OSDs immediately
                up_osds[osd_num] = true;
            }
            tree[osd_num] = tree[osd_num] || {};
            tree[osd_num].id = osd_num;
            tree[osd_num].parent = tree[osd_num].parent || stat.host;
            tree[osd_num].level = 'osd';
            tree[osd_num].size = reweight * stat.size / 1024 / 1024 / 1024 / 1024; // terabytes
            if (osd_cfg && osd_cfg.tags)
            {
                tree[osd_num].tags = (osd_cfg.tags instanceof Array ? [ ...osd_cfg.tags ] : [ osd_cfg.tags ])
                    .reduce((a, c) => { a[c] = true; return a; }, {});
            }
            delete tree[osd_num].children;
            if (!tree[stat.host])
            {
                tree[stat.host] = {
                    id: stat.host,
                    level: 'host',
                    parent: null,
                    children: [],
                };
            }
        }
    }
    for (const node_id in state.config.node_placement||{})
    {
        const node_cfg = state.config.node_placement[node_id];
        if (/^\d+$/.exec(node_id))
        {
            node_cfg.level = 'osd';
        }
        if (!node_id || !node_cfg.level || !levels[node_cfg.level] ||
            node_cfg.level === 'osd' && !tree[node_id])
        {
            // All nodes must have non-empty IDs and valid levels
            // OSDs have to actually exist
            continue;
        }
        tree[node_id] = tree[node_id] || {};
        tree[node_id].id = node_id;
        tree[node_id].level = node_cfg.level;
        tree[node_id].parent = node_cfg.parent;
        if (node_cfg.level !== 'osd')
        {
            tree[node_id].children = [];
        }
    }
    return { up_osds, levels, osd_tree: tree };
}

function make_hier_tree(global_config, tree)
{
    const levels = global_config.placement_levels||{};
    levels.host = levels.host || 100;
    levels.osd = levels.osd || 101;
    tree = { ...tree };
    for (const node_id in tree)
    {
        tree[node_id] = { ...tree[node_id], children: [] };
    }
    tree[''] = { children: [] };
    for (const node_id in tree)
    {
        if (node_id === '')
        {
            continue;
        }
        const node_cfg = tree[node_id];
        const node_level = levels[node_cfg.level] || node_cfg.level;
        let parent_level = node_cfg.parent && tree[node_cfg.parent] && tree[node_cfg.parent].children
            && tree[node_cfg.parent].level;
        parent_level = parent_level ? (levels[parent_level] || parent_level) : null;
        // Parent's level must be less than child's; OSDs must be leaves
        const parent = parent_level && parent_level < node_level ? node_cfg.parent : '';
        tree[parent].children.push(tree[node_id]);
    }
    // Delete empty nodes
    let deleted = 0;
    do
    {
        deleted = 0;
        for (const node_id in tree)
        {
            if (!(tree[node_id].children||[]).length && (tree[node_id].size||0) <= 0)
            {
                const parent = tree[node_id].parent;
                if (parent && tree[parent])
                {
                    tree[parent].children = tree[parent].children.filter(c => c != tree[node_id]);
                }
                deleted++;
                delete tree[node_id];
            }
        }
    } while (deleted > 0);
    return tree;
}

function filter_osds_by_root_node(global_config, pool_tree, root_node)
{
    if (!root_node)
    {
        return;
    }
    let hier_tree = make_hier_tree(global_config, pool_tree);
    let included = [ ...(hier_tree[root_node] || {}).children||[] ];
    for (let i = 0; i < included.length; i++)
    {
        if (included[i].children)
        {
            included.splice(i+1, 0, ...included[i].children);
        }
    }
    let cur = pool_tree[root_node] || {};
    while (cur && cur.id)
    {
        included.unshift(cur);
        cur = pool_tree[cur.parent||''];
    }
    included = included.reduce((a, c) => { a[c.id||''] = true; return a; }, {});
    for (const item in pool_tree)
    {
        if (!included[item])
        {
            delete pool_tree[item];
        }
    }
}

function filter_osds_by_tags(orig_tree, tags)
{
    if (!tags)
    {
        return;
    }
    for (const tag of (tags instanceof Array ? tags : [ tags ]))
    {
        for (const osd in orig_tree)
        {
            if (orig_tree[osd].level === 'osd' &&
                (!orig_tree[osd].tags || !orig_tree[osd].tags[tag]))
            {
                delete orig_tree[osd];
            }
        }
    }
}

function filter_osds_by_block_layout(orig_tree, osd_stats, block_size, bitmap_granularity, immediate_commit)
{
    for (const osd in orig_tree)
    {
        if (orig_tree[osd].level === 'osd')
        {
            const osd_stat = osd_stats[osd];
            if (osd_stat && (osd_stat.data_block_size && osd_stat.data_block_size != block_size ||
                osd_stat.bitmap_granularity && osd_stat.bitmap_granularity != bitmap_granularity ||
                osd_stat.immediate_commit == 'small' && immediate_commit == 'all' ||
                osd_stat.immediate_commit == 'none' && immediate_commit != 'none'))
            {
                delete orig_tree[osd];
            }
        }
    }
}

function get_affinity_osds(pool_cfg, up_osds, osd_tree)
{
    let aff_osds = up_osds;
    if (pool_cfg.primary_affinity_tags)
    {
        aff_osds = Object.keys(up_osds).reduce((a, c) => { a[c] = osd_tree[c]; return a; }, {});
        filter_osds_by_tags(aff_osds, pool_cfg.primary_affinity_tags);
        for (const osd in aff_osds)
        {
            aff_osds[osd] = true;
        }
    }
    return aff_osds;
}

module.exports = {
    get_osd_tree,
    make_hier_tree,
    filter_osds_by_root_node,
    filter_osds_by_tags,
    filter_osds_by_block_layout,
    get_affinity_osds,
};
