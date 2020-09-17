// Copyright (c) Vitaliy Filippov, 2019+
// License: MIT

function stableStringify(obj, opts)
{
    if (!opts)
        opts = {};
    if (typeof opts === 'function')
        opts = { cmp: opts };
    let space = opts.space || '';
    if (typeof space === 'number')
        space = Array(space+1).join(' ');
    const cycles = (typeof opts.cycles === 'boolean') ? opts.cycles : false;
    const cmp = opts.cmp && (function (f)
    {
        return function (node)
        {
            return function (a, b)
            {
                let aobj = { key: a, value: node[a] };
                let bobj = { key: b, value: node[b] };
                return f(aobj, bobj);
            };
        };
    })(opts.cmp);
    const seen = new Map();
    return (function stringify (parent, key, node, level)
    {
        const indent = space ? ('\n' + new Array(level + 1).join(space)) : '';
        const colonSeparator = space ? ': ' : ':';
        if (node === undefined)
        {
            return;
        }
        if (typeof node !== 'object' || node === null)
        {
            return JSON.stringify(node);
        }
        if (node instanceof Array)
        {
            const out = [];
            for (let i = 0; i < node.length; i++)
            {
                const item = stringify(node, i, node[i], level+1) || JSON.stringify(null);
                out.push(indent + space + item);
            }
            return '[' + out.join(',') + indent + ']';
        }
        else
        {
            if (seen.has(node))
            {
                if (cycles)
                    return JSON.stringify('__cycle__');
                throw new TypeError('Converting circular structure to JSON');
            }
            else
                seen.set(node, true);
            const keys = Object.keys(node).sort(cmp && cmp(node));
            const out = [];
            for (let i = 0; i < keys.length; i++)
            {
                const key = keys[i];
                const value = stringify(node, key, node[key], level+1);
                if (!value)
                    continue;
                const keyValue = JSON.stringify(key)
                    + colonSeparator
                    + value;
                out.push(indent + space + keyValue);
            }
            seen.delete(node);
            return '{' + out.join(',') + indent + '}';
        }
    })({ '': obj }, '', obj, 0);
}

module.exports = stableStringify;
