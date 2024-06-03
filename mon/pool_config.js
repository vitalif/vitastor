// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const { parse_level_indexes, parse_pg_dsl } = require('./dsl_pgs.js');

function validate_pool_cfg(pool_id, pool_cfg, placement_levels, warn)
{
    pool_cfg.pg_size = Math.floor(pool_cfg.pg_size);
    pool_cfg.pg_minsize = Math.floor(pool_cfg.pg_minsize);
    pool_cfg.parity_chunks = Math.floor(pool_cfg.parity_chunks) || undefined;
    pool_cfg.pg_count = Math.floor(pool_cfg.pg_count);
    pool_cfg.max_osd_combinations = Math.floor(pool_cfg.max_osd_combinations) || 10000;
    if (!/^[1-9]\d*$/.exec(''+pool_id))
    {
        if (warn)
            console.log('Pool ID '+pool_id+' is invalid');
        return false;
    }
    if (pool_cfg.scheme !== 'xor' && pool_cfg.scheme !== 'replicated' &&
        pool_cfg.scheme !== 'ec' && pool_cfg.scheme !== 'jerasure')
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid coding scheme (one of "xor", "replicated", "ec" and "jerasure" required)');
        return false;
    }
    if (!pool_cfg.pg_size || pool_cfg.pg_size < 1 || pool_cfg.pg_size > 256 ||
        pool_cfg.scheme !== 'replicated' && pool_cfg.pg_size < 3)
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid pg_size');
        return false;
    }
    if (!pool_cfg.pg_minsize || pool_cfg.pg_minsize < 1 || pool_cfg.pg_minsize > pool_cfg.pg_size ||
        pool_cfg.scheme === 'xor' && pool_cfg.pg_minsize < (pool_cfg.pg_size - 1))
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid pg_minsize');
        return false;
    }
    if (pool_cfg.scheme === 'xor' && pool_cfg.parity_chunks != 0 && pool_cfg.parity_chunks != 1)
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid parity_chunks (must be 1)');
        return false;
    }
    if ((pool_cfg.scheme === 'ec' || pool_cfg.scheme === 'jerasure') &&
        (pool_cfg.parity_chunks < 1 || pool_cfg.parity_chunks > pool_cfg.pg_size-2))
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid parity_chunks (must be between 1 and pg_size-2)');
        return false;
    }
    if (!pool_cfg.pg_count || pool_cfg.pg_count < 1)
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid pg_count');
        return false;
    }
    if (!pool_cfg.name)
    {
        if (warn)
            console.log('Pool '+pool_id+' has empty name');
        return false;
    }
    if (pool_cfg.max_osd_combinations < 100)
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid max_osd_combinations (must be at least 100)');
        return false;
    }
    if (pool_cfg.root_node && typeof(pool_cfg.root_node) != 'string')
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid root_node (must be a string)');
        return false;
    }
    if (pool_cfg.osd_tags && typeof(pool_cfg.osd_tags) != 'string' &&
        (!(pool_cfg.osd_tags instanceof Array) || pool_cfg.osd_tags.filter(t => typeof t != 'string').length > 0))
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid osd_tags (must be a string or array of strings)');
        return false;
    }
    if (pool_cfg.primary_affinity_tags && typeof(pool_cfg.primary_affinity_tags) != 'string' &&
        (!(pool_cfg.primary_affinity_tags instanceof Array) || pool_cfg.primary_affinity_tags.filter(t => typeof t != 'string').length > 0))
    {
        if (warn)
            console.log('Pool '+pool_id+' has invalid primary_affinity_tags (must be a string or array of strings)');
        return false;
    }
    if (!get_pg_rules(pool_id, pool_cfg, placement_levels, true))
    {
        return false;
    }
    return true;
}

function get_pg_rules(pool_id, pool_cfg, placement_levels, warn)
{
    if (pool_cfg.level_placement)
    {
        const pg_size = (0|pool_cfg.pg_size);
        let rules = pool_cfg.level_placement;
        if (typeof rules === 'string')
        {
            rules = rules.split(/\s+/).map(s => s.split(/=/, 2)).reduce((a, c) => { a[c[0]] = c[1]; return a; }, {});
        }
        else
        {
            rules = { ...rules };
        }
        // Always add failure_domain to prevent rules from being totally incorrect
        const all_diff = [];
        for (let i = 1; i <= pg_size; i++)
        {
            all_diff.push(i);
        }
        rules[pool_cfg.failure_domain || 'host'] = all_diff;
        placement_levels = placement_levels||{};
        placement_levels.host = placement_levels.host || 100;
        placement_levels.osd = placement_levels.osd || 101;
        for (const k in rules)
        {
            if (!placement_levels[k] || typeof rules[k] !== 'string' &&
                (!(rules[k] instanceof Array) ||
                rules[k].filter(s => typeof s !== 'string' && typeof s !== 'number').length > 0))
            {
                if (warn)
                    console.log('Pool '+pool_id+' configuration is invalid: level_placement should be { [level]: string | (string|number)[] }');
                return null;
            }
            else if (rules[k].length != pg_size)
            {
                if (warn)
                    console.log('Pool '+pool_id+' configuration is invalid: values in level_placement should contain exactly pg_size ('+pg_size+') items');
                return null;
            }
        }
        return parse_level_indexes(rules);
    }
    else if (typeof pool_cfg.raw_placement === 'string')
    {
        try
        {
            return parse_pg_dsl(pool_cfg.raw_placement);
        }
        catch (e)
        {
            if (warn)
                console.log('Pool '+pool_id+' configuration is invalid: invalid raw_placement: '+e.message);
        }
    }
    else
    {
        let rules = [ [] ];
        let prev = [ 1 ];
        for (let i = 1; i < pool_cfg.pg_size; i++)
        {
            rules.push([ [ pool_cfg.failure_domain||'host', '!=', prev ] ]);
            prev = [ ...prev, i+1 ];
        }
        return rules;
    }
}

module.exports = {
    validate_pool_cfg,
    get_pg_rules,
};
