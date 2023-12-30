function select_murmur3(count, cb)
{
    if (!count)
    {
        return null;
    }
    else
    {
        let i = 0, maxh = -1;
        for (let j = 0; j < count; j++)
        {
            const h = murmur3(cb(j));
            if (h > maxh)
            {
                i = j;
                maxh = h;
            }
        }
        return i;
    }
}

function murmur3(s)
{
    let hash = 0x12345678;
    for (let i = 0; i < s.length; i++)
    {
        hash ^= s.charCodeAt(i);
        hash = (hash*0x5bd1e995) & 0xFFFFFFFF;
        hash ^= (hash >> 15);
    }
    return hash;
}

module.exports = {
    murmur3,
    select_murmur3,
};
