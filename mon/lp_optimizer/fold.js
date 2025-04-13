// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

// Extract OSDs from the lowest affected tree level into a separate (flat) map
// to run PG optimisation on failure domains instead of individual OSDs
//
// node_list = same input as for index_tree()
// rules = [ level, operator, value ][][]
// returns { nodes: new_node_list, leaves: { new_folded_node_id: [ extracted_leaf_nodes... ] } }
function fold_failure_domains(node_list, rules)
{
    const interest = {};
    for (const level_rules of rules)
    {
        for (const rule of level_rules)
            interest[rule[0]] = true;
    }
    const max_numeric_id = node_list.reduce((a, c) => a < (0|c.id) ? (0|c.id) : a, 0);
    let next_id = max_numeric_id;
    const node_map = node_list.reduce((a, c) => { a[c.id||''] = c; return a; }, {});
    const old_ids_by_new = {};
    const extracted_nodes = {};
    let folded = true;
    while (folded)
    {
        const per_parent = {};
        for (const node_id in node_map)
        {
            const node = node_map[node_id];
            const p = node.parent || '';
            per_parent[p] = per_parent[p]||[];
            per_parent[p].push(node);
        }
        folded = false;
        for (const node_id in per_parent)
        {
            const fold_node = per_parent[node_id].filter(child => per_parent[child.id||''] || interest[child.level]).length == 0;
            if (fold_node)
            {
                const old_node = node_map[node_id];
                const new_id = ++next_id;
                node_map[new_id] = {
                    ...old_node,
                    id: new_id,
                    name: node_id, // for use in murmur3 hashes
                    size: per_parent[node_id].reduce((a, c) => a + (Number(c.size)||0), 0),
                };
                delete node_map[node_id];
                old_ids_by_new[new_id] = node_id;
                extracted_nodes[new_id] = [];
                for (const child of per_parent[node_id])
                {
                    if (old_ids_by_new[child.id])
                    {
                        extracted_nodes[new_id].push(...extracted_nodes[child.id]);
                        delete extracted_nodes[child.id];
                    }
                    else
                        extracted_nodes[new_id].push(child);
                    delete node_map[child.id];
                }
                folded = true;
            }
        }
    }
    return { nodes: Object.values(node_map), leaves: extracted_nodes };
}

// Distribute PGs mapped to "folded" nodes to individual OSDs according to their weights
// folded_pgs = optimize_result.int_pgs before folding
// prev_pgs = optional previous PGs from optimize_change() input
// extracted_nodes = output from fold_failure_domains
function unfold_failure_domains(folded_pgs, prev_pgs, extracted_nodes)
{
    const maps = {};
    let found = false;
    for (const new_id in extracted_nodes)
    {
        const weights = {};
        for (const sub_node of extracted_nodes[new_id])
        {
            weights[sub_node.id] = sub_node.size;
        }
        maps[new_id] = { weights, prev: [], next: [], pos: 0 };
        found = true;
    }
    if (!found)
    {
        return folded_pgs;
    }
    for (let i = 0; i < folded_pgs.length; i++)
    {
        for (let j = 0; j < folded_pgs[i].length; j++)
        {
            if (maps[folded_pgs[i][j]])
            {
                maps[folded_pgs[i][j]].prev.push(prev_pgs && prev_pgs[i] && prev_pgs[i][j] || 0);
            }
        }
    }
    for (const new_id in maps)
    {
        maps[new_id].next = adjust_distribution(maps[new_id].weights, maps[new_id].prev);
    }
    const mapped_pgs = [];
    for (let i = 0; i < folded_pgs.length; i++)
    {
        mapped_pgs.push(folded_pgs[i].map(osd => (maps[osd] ? maps[osd].next[maps[osd].pos++] : osd)));
    }
    return mapped_pgs;
}

// Return the new array of items re-distributed as close as possible to weights in wanted_weights
// wanted_weights = { [key]: weight }
// cur_items = key[]
function adjust_distribution(wanted_weights, cur_items)
{
    const item_map = {};
    for (let i = 0; i < cur_items.length; i++)
    {
        const item = cur_items[i];
        item_map[item] = (item_map[item] || { target: 0, cur: [] });
        item_map[item].cur.push(i);
    }
    let total_weight = 0;
    for (const item in wanted_weights)
    {
        total_weight += Number(wanted_weights[item]) || 0;
    }
    for (const item in wanted_weights)
    {
        const weight = wanted_weights[item] / total_weight * cur_items.length;
        if (weight > 0)
        {
            item_map[item] = (item_map[item] || { target: 0, cur: [] });
            item_map[item].target = weight;
        }
    }
    const diff = (item) => (item_map[item].cur.length - item_map[item].target);
    const most_underweighted = Object.keys(item_map)
        .filter(item => item_map[item].target > 0)
        .sort((a, b) => diff(a) - diff(b));
    // Items with zero target weight MUST never be selected - remove them
    // and remap each of them to a most underweighted item
    for (const item in item_map)
    {
        if (!item_map[item].target)
        {
            const prev = item_map[item];
            delete item_map[item];
            for (const idx of prev.cur)
            {
                const move_to = most_underweighted[0];
                item_map[move_to].cur.push(idx);
                move_leftmost(most_underweighted, diff);
            }
        }
    }
    // Other over-weighted items are only moved if it improves the distribution
    while (most_underweighted.length > 1)
    {
        const first = most_underweighted[0];
        const last = most_underweighted[most_underweighted.length-1];
        const first_diff = diff(first);
        const last_diff = diff(last);
        if (Math.abs(first_diff+1)+Math.abs(last_diff-1) < Math.abs(first_diff)+Math.abs(last_diff))
        {
            item_map[first].cur.push(item_map[last].cur.pop());
            move_leftmost(most_underweighted, diff);
            move_rightmost(most_underweighted, diff);
        }
        else
        {
            break;
        }
    }
    const new_items = new Array(cur_items.length);
    for (const item in item_map)
    {
        for (const idx of item_map[item].cur)
        {
            new_items[idx] = item;
        }
    }
    return new_items;
}

function move_leftmost(sorted_array, diff)
{
    // Re-sort by moving the leftmost item to the right if it changes position
    const first = sorted_array[0];
    const new_diff = diff(first);
    let r = 0;
    while (r < sorted_array.length-1 && diff(sorted_array[r+1]) <= new_diff)
        r++;
    if (r > 0)
    {
        for (let i = 0; i < r; i++)
            sorted_array[i] = sorted_array[i+1];
        sorted_array[r] = first;
    }
}

function move_rightmost(sorted_array, diff)
{
    // Re-sort by moving the rightmost item to the left if it changes position
    const last = sorted_array[sorted_array.length-1];
    const new_diff = diff(last);
    let r = sorted_array.length-1;
    while (r > 0 && diff(sorted_array[r-1]) > new_diff)
        r--;
    if (r < sorted_array.length-1)
    {
        for (let i = sorted_array.length-1; i > r; i--)
            sorted_array[i] = sorted_array[i-1];
        sorted_array[r] = last;
    }
}

// map previous PGs to folded nodes
function fold_prev_pgs(pgs, extracted_nodes)
{
    const unmap = {};
    for (const new_id in extracted_nodes)
    {
        for (const sub_node of extracted_nodes[new_id])
        {
            unmap[sub_node.id] = new_id;
        }
    }
    const mapped_pgs = [];
    for (let i = 0; i < pgs.length; i++)
    {
        mapped_pgs.push(pgs[i].map(osd => (unmap[osd] || osd)));
    }
    return mapped_pgs;
}

module.exports = {
    fold_failure_domains,
    unfold_failure_domains,
    adjust_distribution,
    fold_prev_pgs,
};
