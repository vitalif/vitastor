#!/usr/bin/nodejs

const fsp = require('fs').promises;

run(process.argv).catch(console.error);

async function run(argv)
{
    if (argv.length < 3)
    {
        console.log('Markdown preprocessor\nUSAGE: ./include.js file.md');
        return;
    }
    const index_file = await fsp.realpath(argv[2]);
    const re = /(\{\{[\s\S]*?\}\}|\[[^\]]+\]\([^\)]+\)|(?:^|\n)#[^\n]+)/;
    let text = await fsp.readFile(index_file, { encoding: 'utf-8' });
    text = text.split(re);
    let included = {};
    let heading = 0, heading_name = '', m;
    for (let i = 0; i < text.length; i++)
    {
        if (text[i].substr(0, 2) == '{{')
        {
            // Inclusion
            let incfile = text[i].substr(2, text[i].length-4);
            let section = null;
            let indent = heading;
            incfile = incfile.replace(/\s*\|\s*indent\s*=\s*(-?\d+)\s*$/, (m, m1) => { indent = parseInt(m1); return ''; });
            incfile = incfile.replace(/\s*#\s*([^#]+)$/, (m, m1) => { section = m1; return ''; });
            let inc_heading = section;
            incfile = rel2abs(index_file, incfile);
            let inc = await fsp.readFile(incfile, { encoding: 'utf-8' });
            inc = inc.trim().replace(/^[\s\S]+?\n#/, '#'); // remove until the first header
            inc = inc.split(re);
            const indent_str = new Array(indent+1).join('#');
            let section_start = -1, section_end = -1;
            for (let j = 0; j < inc.length; j++)
            {
                if ((m = /^(\n?)(#+\s*)([\s\S]+)$/.exec(inc[j])))
                {
                    if (!inc_heading)
                    {
                        inc_heading = m[3].trim();
                    }
                    if (section)
                    {
                        if (m[3].trim() == section)
                            section_start = j;
                        else if (section_start >= 0)
                        {
                            section_end = j;
                            break;
                        }
                    }
                    inc[j] = m[1] + indent_str + m[2] + m[3];
                }
                else if ((m = /^(\[[^\]]+\]\()([^\)]+)(\))$/.exec(inc[j])) && !/^https?:(\/\/)|^#/.exec(m[2]))
                {
                    const abs_m2 = rel2abs(incfile, m[2]);
                    const rel_m = abs2rel(__filename, abs_m2);
                    if (rel_m.substr(0, 9) == '../../../') // outside docs
                        inc[j] = m[1] + 'https://git.yourcmc.ru/vitalif/vitastor/src/branch/master/'+rel2abs('docs/config/src/include.js', rel_m) + m[3];
                    else
                        inc[j] = m[1] + abs_m2 + m[3];
                }
            }
            if (section)
            {
                inc = section_start >= 0 ? inc.slice(section_start, section_end < 0 ? inc.length : section_end) : [];
            }
            if (inc.length)
            {
                if (!inc_heading)
                    inc_heading = heading_name||'';
                included[incfile+(section ? '#'+section : '')] = '#'+inc_heading.toLowerCase().replace(/\P{L}+/ug, '-').replace(/^-|-$/g, '');
                inc[0] = inc[0].replace(/^\s+/, '');
                inc[inc.length-1] = inc[inc.length-1].replace(/\s+$/, '');
            }
            text.splice(i, 1, ...inc);
            i = i + inc.length - 1;
        }
        else if ((m = /^\n?(#+)\s*([\s\S]+)$/.exec(text[i])))
        {
            // Heading
            heading = m[1].length;
            heading_name = m[2].trim();
        }
    }
    for (let i = 0; i < text.length; i++)
    {
        if ((m = /^(\[[^\]]+\]\()([^\)]+)(\))$/.exec(text[i])) && !/^https?:(\/\/)|^#/.exec(m[2]))
        {
            const p = m[2].indexOf('#');
            if (included[m[2]])
            {
                text[i] = m[1]+included[m[2]]+m[3];
            }
            else if (p >= 0 && included[m[2].substr(0, p)])
            {
                text[i] = m[1]+m[2].substr(p)+m[3];
            }
        }
    }
    console.log(text.join(''));
}

function rel2abs(ref, rel)
{
    rel = [ ...ref.replace(/^(.*)\/[^\/]+$/, '$1').split(/\/+/), ...rel.split(/\/+/) ];
    return killdots(rel).join('/');
}

function abs2rel(ref, abs)
{
    ref = ref.split(/\/+/);
    abs = abs.split(/\/+/);
    while (ref.length > 1 && ref[0] == abs[0])
    {
        ref.shift();
        abs.shift();
    }
    for (let i = 1; i < ref.length; i++)
    {
        abs.unshift('..');
    }
    return killdots(abs).join('/');
}

function killdots(rel)
{
    for (let i = 0; i < rel.length; i++)
    {
        if (rel[i] == '.')
        {
            rel.splice(i, 1);
            i--;
        }
        else if (i >= 1 && rel[i] == '..' && rel[i-1] != '..')
        {
            rel.splice(i-1, 2);
            i -= 2;
        }
    }
    return rel;
}
