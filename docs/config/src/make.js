#!/usr/bin/nodejs

const fs = require('fs');
const yaml = require('yaml');

const L = {
    en: {
        Documentation: 'Documentation',
        Configuration: 'Configuration',
        Crossref: 'Read in English',
        toc_root: '[Documentation](../README.md#documentation)',
        toc_intro: 'Introduction',
        toc_installation: 'Installation',
        toc_config: '[Configuration](../config.en.md)',
        toc_usage: 'Usage',
        toc_performance: 'Performance',
    },
    ru: {
        Documentation: 'Документация',
        Configuration: 'Конфигурация',
        Type: 'Тип',
        Default: 'Значение по умолчанию',
        Minimum: 'Минимальное значение',
        Crossref: 'Читать на русском',
        toc_root: '[Документация](../README-ru.md#документация)',
        toc_intro: 'Введение',
        toc_installation: 'Установка',
        toc_config: '[Конфигурация](../config.ru.md)',
        toc_usage: 'Использование',
        toc_performance: 'Производительность',
    },
};
const types = {
    en: {
        string: 'string',
        bool: 'boolean',
        int: 'integer',
        sec: 'seconds',
        ms: 'milliseconds',
        us: 'microseconds',
    },
    ru: {
        string: 'строка',
        bool: 'булево (да/нет)',
        int: 'целое число',
        sec: 'секунды',
        ms: 'миллисекунды',
        us: 'микросекунды',
    },
};
const params_files = fs.readdirSync(__dirname)
    .filter(f => f.substr(-4) == '.yml')
    .map(f => f.substr(0, f.length-4));

for (const file of params_files)
{
    const cfg = yaml.parse(fs.readFileSync(__dirname+'/'+file+'.yml', { encoding: 'utf-8' }));
    for (const lang in types)
    {
        let out = '\n';
        for (const c of cfg)
        {
            out += `\n- [${c.name}](#${c.name})`;
        }
        for (const c of cfg)
        {
            out += `\n\n## ${c.name}\n\n`;
            out += `- ${L[lang]['Type'] || 'Type'}: ${c["type_"+lang] || types[lang][c.type] || c.type}\n`;
            if (c.default !== undefined)
                out += `- ${L[lang]['Default'] || 'Default'}: ${c.default}\n`;
            if (c.min !== undefined)
                out += `- ${L[lang]['Minimum'] || 'Minimum'}: ${c.min}\n`;
            out += `\n`+(c["info_"+lang] || c["info"]).replace(/\s+$/, '');
        }
        const head = fs.readFileSync(__dirname+'/'+file+'.'+lang+'.md', { encoding: 'utf-8' });
        out = head.replace(/\s+$/, '')+out+"\n";
        fs.writeFileSync(__dirname+'/../'+file+'.'+lang+'.md', out);
    }
}

// Add "Read in..." to all other documentation files

for (const file of find_files(__dirname+'/../..', name => name.substr(-3) == '.md' && !/config\/src\//.exec(name)))
{
    const m = /^(?:(.*?)\/)?([^\/]+)\.([^\.]+)\.[^\.]+$/.exec(file);
    if (!m)
        continue;
    const [ , subdir, filename, lang ] = m;
    if (!L[lang])
        continue;
    let text = fs.readFileSync(__dirname+'/../../'+file, { encoding: 'utf-8' });
    const title = /(^|\n)# ([^\n]+)/.exec(text)[2];
    let read_in = Object.keys(L).filter(other => other != lang)
        .map(other => `[${L[other].Crossref}](${filename}.${other}.md)`)
        .join(' ')+'\n\n';
    read_in = L[lang]['toc_root'].replace(/\.\.\//, subdir ? '../../' : '../')+' → '+
        (subdir ? L[lang]['toc_'+subdir]+' → ' : '')+
        title+'\n\n-----\n\n'+
        read_in;
    if (text.substr(0, read_in.length) != read_in)
    {
        fs.writeFileSync(__dirname+'/../../'+file, read_in + (text[0] == '#' ? text : text.replace(/^([\s\S]*?\n)?#/, '#')));
    }
}

function find_files(dir, fn, subdir = '', res = [])
{
    for (const ent of fs.readdirSync(dir+'/'+subdir, { withFileTypes: true }))
    {
        if (ent.isDirectory())
        {
            find_files(dir, fn, subdir ? subdir+'/'+ent.name : ent.name, res);
        }
        else if (fn(subdir ? subdir+'/'+ent.name : ent.name, ent))
        {
            res.push(subdir ? subdir+'/'+ent.name : ent.name);
        }
    }
    return res;
}
