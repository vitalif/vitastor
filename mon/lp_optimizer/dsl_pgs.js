const { select_murmur3 } = require('./murmur3.js');

const NO_OSD = 'Z';

class RuleCombinator
{
    constructor(osd_tree, rules, max_combinations, ordered)
    {
        this.osd_tree = index_tree(Object.values(osd_tree).filter(o => o.id));
        this.rules = rules;
        this.max_combinations = max_combinations;
        this.ordered = ordered;
    }

    random_combinations()
    {
        return random_custom_combinations(this.osd_tree, this.rules, this.max_combinations, this.ordered);
    }

    check_combinations(pgs)
    {
        return check_custom_combinations(this.osd_tree, this.rules, pgs);
    }
}

// Convert alternative "level-index" format to rules
// level_index = { [level: string]: string | string[] }
// level_sequence = optional, levels from upper to lower, i.e. [ 'dc', 'host' ]
// Example: level_index = { dc: "112233", host: "ABCDEF" }
function parse_level_indexes(level_index, level_sequence)
{
    const rules = [];
    const lvl_first = {};
    for (const level in level_index)
    {
        const idx = level_index[level];
        while (rules.length < idx.length)
        {
            rules.push([]);
        }
        const seen = {};
        for (let i = 0; i < idx.length; i++)
        {
            if (!seen[idx[i]])
            {
                const other = Object.values(seen);
                if (other.length)
                {
                    rules[i].push([ level, '!=', other ]);
                }
                seen[idx[i]] = i+1;
            }
            else
            {
                rules[i].push([ level, '=', seen[idx[i]] ]);
            }
        }
        lvl_first[level] = seen;
    }
    if (level_sequence)
    {
        // Prune useless rules for the sake of prettiness
        // For simplicity, call "upper" level DC and "lower" level host
        const level_prio = Object.keys(level_sequence).reduce((a, c) => { a[level_sequence[c]] = c; return a; }, {});
        for (let upper_i = 0; upper_i < level_sequence.length-1; upper_i++)
        {
            const upper_level = level_sequence[upper_i];
            for (let i = 0; i < rules.length; i++)
            {
                const noteq = {};
                for (let k = 0; k < level_index[upper_level].length; k++)
                {
                    // If upper_level[x] is different from upper_level[y]
                    // then lower_level[x] is also different from lower_level[y]
                    if (level_index[upper_level][k] != level_index[upper_level][i])
                    {
                        noteq[k+1] = true;
                    }
                }
                for (let j = 0; j < rules[i].length; j++)
                {
                    if (level_prio[rules[i][j][0]] != null && level_prio[rules[i][j][0]] > upper_i && rules[i][j][1] == '!=')
                    {
                        rules[i][j][2] = rules[i][j][2].filter(other_host => !noteq[other_host]);
                        if (!rules[i][j][2].length)
                        {
                            rules[i].splice(j--, 1);
                        }
                    }
                }
            }
        }
    }
    return rules;
}

// Parse rules in DSL format
// dsl := item | item ("\n" | ",") items
// item := "any" | rules
// rules := rule | rule rules
// rule := level operator arg
// level := /\w+/
// operator := "!=" | "=" | ">" | "?="
// arg := value | "(" values ")"
// values := value | value "," values
// value := item_ref | constant_id
// item_ref := /\d+/
// constant_id := /"([^"]+)"/
//
// Output: [ level, operator, value ][][]
function parse_pg_dsl(text)
{
    const tokens = [ ...text.matchAll(/\w+|!=|\?=|[>=\(\),\n]|"([^\"]+)"/g) ].map(t => [ t[0], t.index ]);
    let positions = [ [] ];
    let rules = positions[0];
    for (let i = 0; i < tokens.length; )
    {
        if (tokens[i][0] === '\n' || tokens[i][0] === ',')
        {
            rules = [];
            positions.push(rules);
            i++;
        }
        else if (!rules.length && tokens[i][0] === 'any' && (i == tokens.length-1 || tokens[i+1][0] === ',' || tokens[i+1][0] === '\n'))
        {
            i++;
        }
        else
        {
            if (!/^\w/.exec(tokens[i][0]))
            {
                throw new Error('Unexpected '+tokens[i][0]+' at '+tokens[i][1]+' (level name expected)');
            }
            if (i > tokens.length-3)
            {
                throw new Error('Unexpected EOF (operator and value expected)');
            }
            if (/^\w/.exec(tokens[i+1][0]) || tokens[i+1][0] === ',' || tokens[i+1][0] === '\n')
            {
                throw new Error('Unexpected '+tokens[i+1][0]+' at '+tokens[i+1][1]+' (operator expected)');
            }
            if (!/^[\w"(]/.exec(tokens[i+2][0])) // "
            {
                throw new Error('Unexpected '+tokens[i+2][0]+' at '+tokens[i+2][1]+' (id, round brace, number or node ID expected)');
            }
            let rule = [ tokens[i][0], tokens[i+1][0], tokens[i+2][0] ];
            i += 3;
            if (rule[2][0] == '"')
            {
                rule[2] = { id: rule[2].substr(1, rule[2].length-2) };
            }
            else if (rule[2] === '(')
            {
                rule[2] = [];
                // eslint-disable-next-line no-constant-condition
                while (true)
                {
                    if (i > tokens.length-1)
                    {
                        throw new Error('Unexpected EOF (expected list and a closing round brace)');
                    }
                    if (tokens[i][0] === ',')
                    {
                        i++;
                    }
                    else if (tokens[i][0] === ')')
                    {
                        i++;
                        break;
                    }
                    else if (tokens[i][0][0] === '"')
                    {
                        rule[2].push({ id: tokens[i][0].substr(1, tokens[i][0].length-2) });
                        i++;
                    }
                    else if (/^\d+$/.exec(tokens[i][0]))
                    {
                        const n = 0|tokens[i][0];
                        if (!n)
                        {
                            throw new Error('Level reference cannot be 0 (refs count from 1) at '+tokens[i][1]);
                        }
                        else if (n > positions.length)
                        {
                            throw new Error('Forward references are forbidden at '+tokens[i][1]);
                        }
                        rule[2].push(n);
                        i++;
                    }
                    else if (!/^\w/.exec(tokens[i][0]))
                    {
                        throw new Error('Unexpected '+tokens[i][0]+' at '+tokens[i][1]+' (number or node ID expected)');
                    }
                    else
                    {
                        rule[2].push({ id: tokens[i][0] });
                        i++;
                    }
                }
            }
            else if (!/^\d+$/.exec(rule[2]))
            {
                rule[2] = { id: rule[2] };
            }
            else
            {
                rule[2] = 0|rule[2];
                if (!rule[2])
                {
                    throw new Error('Level reference cannot be 0 (refs count from 1) at '+tokens[i-1][1]);
                }
                else if (rule[2] > positions.length)
                {
                    throw new Error('Forward references are forbidden at '+tokens[i-1][1]);
                }
            }
            rules.push(rule);
        }
    }
    return positions;
}

// osd_tree = index_tree() output
// levels = { string: number }
// rules = [ level, operator, value ][][]
//   level = string
//   operator = '=' | '!=' | '>' | '?='
//   value = number|number[] | { id: string|string[] }
// examples:
// 1) simple 3 replicas with failure_domain=host:
//    [ [], [ [ 'host', '!=', 1 ] ], [ [ 'host', '!=', [ 1, 2 ] ] ] ]
//    in DSL form: any, host!=1, host!=(1,2)
// 2) EC 4+2 in 3 DC:
//    [ [], [ [ 'dc', '=', 1 ], [ 'host', '!=', 1 ] ],
//      [ 'dc', '!=', 1 ], [ [ 'dc', '=', 3 ], [ 'host', '!=', 3 ] ],
//      [ 'dc', '!=', [ 1, 3 ] ], [ [ 'dc', '=', 5 ], [ 'host', '!=', 5 ] ] ]
//    in DSL form: any, dc=1 host!=1, dc!=1, dc=3 host!=3, dc!=(1,3), dc=5 host!=5
// 3) 1 replica in fixed DC + 2 in random DCs:
//    [ [ [ 'dc', '=', { id: 'meow' } ] ], [ [ 'dc', '!=', 1 ] ], [ [ 'dc', '!=', [ 1, 2 ] ] ] ]
//    in DSL form: dc=meow, dc!=1, dc!=(1,2)
// 4) 2 replicas in each DC (almost the same as (2)):
//    DSL: any, dc=1 host!=1, dc!=1, dc=3 host!=3
// Alternative simpler way to specify rules would be: [ DC: 112233 HOST: 123456 ]
function random_custom_combinations(osd_tree, rules, count, ordered)
{
    const r = {};
    const first = filter_tree_by_rules(osd_tree, rules[0], []);
    let max_size = 0;
    // All combinations for the first item (usually "any") to try to include each OSD at least once
    for (const f of first)
    {
        const selected = [ f ];
        for (let i = 1; i < rules.length; i++)
        {
            const filtered = filter_tree_by_rules(osd_tree, rules[i], selected);
            const idx = select_murmur3(filtered.length, i => 'p:'+f.id+':'+(filtered[i].name || filtered[i].id));
            selected.push(idx == null ? { levels: {}, id: null } : filtered[idx]);
        }
        const size = selected.filter(s => s.id !== null).length;
        max_size = max_size < size ? size : max_size;
        const pg = selected.map(s => s.id === null ? NO_OSD : (0|s.id));
        if (!ordered)
            pg.sort();
        r['pg_'+pg.join('_')] = pg;
    }
    // Pseudo-random selection
    for (let n = 0; n < count; n++)
    {
        const selected = [];
        for (const item_rules of rules)
        {
            const filtered = selected.length ? filter_tree_by_rules(osd_tree, item_rules, selected) : first;
            const idx = select_murmur3(filtered.length, i => n+':'+(filtered[i].name || filtered[i].id));
            selected.push(idx == null ? { levels: {}, id: null } : filtered[idx]);
        }
        const size = selected.filter(s => s.id !== null).length;
        max_size = max_size < size ? size : max_size;
        const pg = selected.map(s => s.id === null ? NO_OSD : (0|s.id));
        if (!ordered)
            pg.sort();
        r['pg_'+pg.join('_')] = pg;
    }
    // Exclude PGs with less successful selections than maximum
    for (const k in r)
    {
        if (r[k].filter(s => s !== NO_OSD).length < max_size)
        {
            delete r[k];
        }
    }
    return r;
}

function filter_tree_by_rules(osd_tree, rules, selected)
{
    let cur = osd_tree[''].children;
    for (const rule of rules)
    {
        const val = (rule[2] instanceof Array ? rule[2] : [ rule[2] ])
            .map(v => v instanceof Object ? v.id : selected[v-1].levels[rule[0]]);
        let preferred = [], other = [];
        for (let i = 0; i < cur.length; i++)
        {
            const item = cur[i];
            const level_id = item.levels[rule[0]];
            if (level_id)
            {
                if (rule[1] == '>' && val.filter(v => level_id <= v).length == 0 ||
                    (rule[1] == '=' || rule[1] == '?=') && val.filter(v => level_id != v).length == 0 ||
                    rule[1] == '!=' && val.filter(v => level_id == v).length == 0)
                {
                    // Include
                    preferred.push(item);
                }
                else if (rule[1] == '?=' && val.filter(v => level_id != v).length > 0)
                {
                    // Non-preferred
                    other.push(item);
                }
            }
            else if (item.children)
            {
                // Descend
                cur.splice(i+1, 0, ...item.children);
            }
        }
        cur = preferred.length ? preferred : other;
    }
    // Get leaf items
    for (let i = 0; i < cur.length; i++)
    {
        if (cur[i].children)
        {
            // Descend
            cur.splice(i, 1, ...cur[i].children);
            i--;
        }
    }
    return cur;
}

// Convert from
// node_list = { id: string|number, name?: string, level: string, size?: number, parent?: string|number }[]
// to
// node_tree = { [node_id]: { id, name?, level, size?, parent?, children?: child_node[], levels: { [level]: id, ... } } }
function index_tree(node_list)
{
    const tree = { '': { children: [], levels: {} } };
    for (const node of node_list)
    {
        tree[node.id] = { ...node, levels: {} };
        delete tree[node.id].children;
    }
    for (const node of node_list)
    {
        const parent_id = node.parent && tree[node.parent] ? node.parent : '';
        tree[parent_id].children = tree[parent_id].children || [];
        tree[parent_id].children.push(tree[node.id]);
    }
    const cur = [ ...tree[''].children ];
    for (let i = 0; i < cur.length; i++)
    {
        cur[i].levels[cur[i].level] = cur[i].id;
        if (cur[i].children)
        {
            for (const child of cur[i].children)
            {
                child.levels = { ...cur[i].levels, ...child.levels };
            }
            cur.splice(i, 1, ...cur[i].children);
            i--;
        }
    }
    return tree;
}

// selection = id[]
// osd_tree = index_tree output
// rules = parse_pg_dsl output
function check_custom_combinations(osd_tree, rules, pgs)
{
    const res = [];
    skip_pg: for (const pg of pgs)
    {
        let selected = pg.map(id => osd_tree[id] || null);
        for (let i = 0; i < rules.length; i++)
        {
            const filtered = filter_tree_by_rules(osd_tree, rules[i], selected);
            if (selected[i] === null && filtered.length ||
                !filtered.filter(ok => selected[i].id === ok.id).length)
            {
                continue skip_pg;
            }
        }
        res.push(pg);
    }
    return res;
}

module.exports = {
    RuleCombinator,
    NO_OSD,

    index_tree,
    parse_level_indexes,
    parse_pg_dsl,
    random_custom_combinations,
    check_custom_combinations,
};
