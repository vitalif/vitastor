const { random_custom_combinations, index_tree, parse_level_indexes, parse_pg_dsl } = require('./dsl_pgs.js');

function check(result, expected)
{
    console.dir(result, { depth: null });
    if (JSON.stringify(result) !== JSON.stringify(expected))
    {
        process.stderr.write('Unexpected value, expected: ');
        console.dir(expected, { depth: null });
        process.exit(1);
    }
}

check(
    parse_pg_dsl("any, dc=1 host!=1, dc!=1, dc=3 host!=3, dc!=(1,3), dc=5 host!=5"),
    [
        [],
        [ [ 'dc', '=', 1 ], [ 'host', '!=', 1 ] ],
        [ [ 'dc', '!=', 1 ] ],
        [ [ 'dc', '=', 3 ], [ 'host', '!=', 3 ] ],
        [ [ 'dc', '!=', [ 1, 3 ] ] ],
        [ [ 'dc', '=', 5 ], [ 'host', '!=', 5 ] ],
    ]
);

check(
    parse_pg_dsl("dc=meow, dc!=1, dc>2"),
    [
        [ [ 'dc', '=', { id: 'meow' } ] ],
        [ [ 'dc', '!=', 1 ] ],
        [ [ 'dc', '>', 2 ] ],
    ]
);

check(
    parse_level_indexes({ dc: '112233', host: 'ABCDEF' }),
    [
        [],
        [ [ 'dc', '=', 1 ],         [ 'host', '!=', [ 1 ] ] ],
        [ [ 'dc', '!=', [ 1 ] ],    [ 'host', '!=', [ 1, 2 ] ] ],
        [ [ 'dc', '=', 3 ],         [ 'host', '!=', [ 1, 2, 3 ] ] ],
        [ [ 'dc', '!=', [ 1, 3 ] ], [ 'host', '!=', [ 1, 2, 3, 4 ] ] ],
        [ [ 'dc', '=', 5 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5 ] ] ],
    ]
);

check(
    parse_level_indexes({ dc: '112233', host: 'ABCDEF' }, [ 'dc', 'host' ]),
    [
        [],
        [ [ 'dc', '=', 1 ],         [ 'host', '!=', [ 1 ] ] ],
        [ [ 'dc', '!=', [ 1 ] ] ],
        [ [ 'dc', '=', 3 ],         [ 'host', '!=', [ 3 ] ] ],
        [ [ 'dc', '!=', [ 1, 3 ] ] ],
        [ [ 'dc', '=', 5 ],         [ 'host', '!=', [ 5 ] ] ],
    ]
);

check(
    parse_level_indexes({ dc: '112211223333', host: '123456789ABC' }),
    [
        [],
        [ [ 'dc', '=', 1 ],         [ 'host', '!=', [ 1 ] ] ],
        [ [ 'dc', '!=', [ 1 ] ],    [ 'host', '!=', [ 1, 2 ] ] ],
        [ [ 'dc', '=', 3 ],         [ 'host', '!=', [ 1, 2, 3 ] ] ],
        [ [ 'dc', '=', 1 ],         [ 'host', '!=', [ 1, 2, 3, 4 ] ] ],
        [ [ 'dc', '=', 1 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5 ] ] ],
        [ [ 'dc', '=', 3 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5, 6 ] ] ],
        [ [ 'dc', '=', 3 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5, 6, 7 ] ] ],
        [ [ 'dc', '!=', [ 1, 3 ] ], [ 'host', '!=', [ 1, 2, 3, 4, 5, 6, 7, 8 ] ] ],
        [ [ 'dc', '=', 9 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ] ],
        [ [ 'dc', '=', 9 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ] ] ],
        [ [ 'dc', '=', 9 ],         [ 'host', '!=', [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ] ] ],
    ]
);

check(
    parse_level_indexes({ dc: '112211223333', host: '123456789ABC' }, [ 'dc', 'host' ]),
    [
        [],
        [ [ 'dc', '=', 1 ], [ 'host', '!=', [ 1 ] ] ],
        [ [ 'dc', '!=', [ 1 ] ] ],
        [ [ 'dc', '=', 3 ], [ 'host', '!=', [ 3 ] ] ],
        [ [ 'dc', '=', 1 ], [ 'host', '!=', [ 1, 2 ] ] ],
        [ [ 'dc', '=', 1 ], [ 'host', '!=', [ 1, 2, 5 ] ] ],
        [ [ 'dc', '=', 3 ], [ 'host', '!=', [ 3, 4 ] ] ],
        [ [ 'dc', '=', 3 ], [ 'host', '!=', [ 3, 4, 7 ] ] ],
        [ [ 'dc', '!=', [ 1, 3 ] ] ],
        [ [ 'dc', '=', 9 ], [ 'host', '!=', [ 9 ] ] ],
        [ [ 'dc', '=', 9 ], [ 'host', '!=', [ 9, 10 ] ] ],
        [ [ 'dc', '=', 9 ], [ 'host', '!=', [ 9, 10, 11 ] ] ]
    ]
);

check(
    Object.keys(random_custom_combinations(index_tree([
        { id: '1', size: 1, level: 'osd' },
        { id: '2', size: 2, level: 'osd' },
        { id: '3', size: 3, level: 'osd' }
    ]), parse_level_indexes({ osd: '12' }), 10000)).sort(),
    [ 'pg_1_2', 'pg_1_3', 'pg_2_3' ]
);

console.log('OK');
