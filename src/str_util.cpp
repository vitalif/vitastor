// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <assert.h>
#include <string.h>
#include <unistd.h>
#include "str_util.h"

std::string base64_encode(const std::string &in)
{
    std::string out;
    unsigned val = 0;
    int valb = -6;
    for (unsigned char c: in)
    {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0)
        {
            out.push_back("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[(val>>valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6)
        out.push_back("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[((val<<8)>>(valb+8)) & 0x3F]);
    while (out.size() % 4)
        out.push_back('=');
    return out;
}

static char T[256] = { 0 };

std::string base64_decode(const std::string &in)
{
    std::string out;
    if (T[0] == 0)
    {
        for (int i = 0; i < 256; i++)
            T[i] = -1;
        for (int i = 0; i < 64; i++)
            T[(unsigned char)("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[i])] = i;
    }
    unsigned val = 0;
    int valb = -8;
    for (unsigned char c: in)
    {
        if (T[c] == -1)
            break;
        val = (val<<6) + T[c];
        valb += 6;
        if (valb >= 0)
        {
            out.push_back(char((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return out;
}

std::string strtoupper(const std::string & in)
{
    std::string s = in;
    for (int i = 0; i < s.length(); i++)
    {
        s[i] = toupper(s[i]);
    }
    return s;
}

std::string strtolower(const std::string & in)
{
    std::string s = in;
    for (int i = 0; i < s.length(); i++)
    {
        s[i] = tolower(s[i]);
    }
    return s;
}

std::string trim(const std::string & in, const char *rm_chars)
{
    int begin = in.find_first_not_of(rm_chars);
    if (begin == -1)
        return "";
    int end = in.find_last_not_of(rm_chars);
    return in.substr(begin, end+1-begin);
}

std::string str_replace(const std::string & in, const std::string & needle, const std::string & replacement)
{
    std::string res;
    int pos = 0, p2;
    while ((p2 = in.find(needle, pos)) >= 0)
    {
        res += in.substr(pos, p2-pos);
        res += replacement;
        pos = p2 + replacement.size();
    }
    if (!pos)
    {
        return in;
    }
    return res + in.substr(pos);
}

uint64_t stoull_full(const std::string & str, int base)
{
    if (isspace(str[0]))
    {
        return 0;
    }
    char *end = NULL;
    uint64_t r = strtoull(str.c_str(), &end, base);
    if (end != str.c_str()+str.length())
    {
        return 0;
    }
    return r;
}

uint64_t parse_size(std::string size_str, bool *ok)
{
    if (!size_str.length())
    {
        if (ok)
            *ok = false;
        return 0;
    }
    uint64_t mul = 1;
    char type_char = tolower(size_str[size_str.length()-1]);
    if (type_char == 'k' || type_char == 'm' || type_char == 'g' || type_char == 't')
    {
        if (type_char == 'k')
            mul = (uint64_t)1<<10;
        else if (type_char == 'm')
            mul = (uint64_t)1<<20;
        else if (type_char == 'g')
            mul = (uint64_t)1<<30;
        else /*if (type_char == 't')*/
            mul = (uint64_t)1<<40;
        size_str = size_str.substr(0, size_str.length()-1);
    }
    uint64_t size = stoull_full(size_str, 0) * mul;
    if (ok)
        *ok = !(size == 0 && size_str != "0" && (size_str != "" || mul != 1));
    return size;
}

static uint64_t size_thresh[] = { (uint64_t)1024*1024*1024*1024, (uint64_t)1024*1024*1024, (uint64_t)1024*1024, 1024, 0 };
static uint64_t size_thresh_d[] = { (uint64_t)1000000000000, (uint64_t)1000000000, (uint64_t)1000000, (uint64_t)1000, 0 };
static const int size_thresh_n = sizeof(size_thresh)/sizeof(size_thresh[0]);
static const char *size_unit = "TGMKB";

std::string format_size(uint64_t size, bool nobytes)
{
    uint64_t *thr = nobytes ? size_thresh_d : size_thresh;
    char buf[256];
    for (int i = 0; i < size_thresh_n; i++)
    {
        if (size >= thr[i] || i >= size_thresh_n-1)
        {
            double value = thr[i] ? (double)size/thr[i] : size;
            int l = snprintf(buf, sizeof(buf), "%.1f", value);
            assert(l < sizeof(buf)-2);
            if (buf[l-1] == '0')
                l -= 2;
            buf[l] = i == size_thresh_n-1 && nobytes ? 0 : ' ';
            buf[l+1] = i == size_thresh_n-1 && nobytes ? 0 : size_unit[i];
            buf[l+2] = 0;
            break;
        }
    }
    return std::string(buf);
}

void print_help(const char *help_text, std::string exe_name, std::string cmd, bool all)
{
    if (cmd == "" && all)
    {
        fwrite(help_text, strlen(help_text), 1, stdout);
        exit(0);
    }
    std::string filtered_text = "";
    const char *head_end = strstr(help_text, "COMMANDS:\n");
    if (head_end)
    {
        filtered_text += std::string(help_text, head_end-help_text);
        head_end += strlen("COMMANDS:\n");
    }
    const char *next_line = head_end ? head_end : help_text;
    if (cmd != "")
    {
        const char *cmd_start = NULL;
        bool matched = false, started = true, found = false;
        while ((next_line = strchr(next_line, '\n')))
        {
            next_line++;
            if (*next_line && !strncmp(next_line, exe_name.c_str(), exe_name.size()))
            {
                if (started)
                {
                    if (cmd_start && matched)
                        filtered_text += std::string(cmd_start, next_line-cmd_start);
                    cmd_start = next_line;
                    matched = started = false;
                }
                const char *var_start = next_line+exe_name.size()+1;
                const char *var_end = var_start;
                while (*var_end && !isspace(*var_end))
                    var_end++;
                if ((std::string(var_start, var_end-var_start)+"|").find(cmd+"|") != std::string::npos)
                    found = matched = true;
            }
            else if (*next_line && isspace(*next_line))
                started = true;
            else if (cmd_start && matched)
            {
                filtered_text += std::string(cmd_start, next_line-cmd_start);
                matched = started = false;
            }
        }
        while (filtered_text.size() > 1 &&
            filtered_text[filtered_text.size()-1] == '\n' &&
            filtered_text[filtered_text.size()-2] == '\n')
        {
            filtered_text.resize(filtered_text.size()-1);
        }
        if (!found)
        {
            filtered_text = "Unknown command: "+cmd+". Use "+exe_name+" --help for usage\n";
        }
    }
    else
    {
        filtered_text += "COMMANDS:\n\n";
        while ((next_line = strchr(next_line, '\n')))
        {
            next_line++;
            if (*next_line && !strncmp(next_line, exe_name.c_str(), exe_name.size()))
            {
                const char *line_end = strchr(next_line, '\n');
                line_end = line_end ? line_end : next_line+strlen(next_line);
                filtered_text += "  "+(line_end ? std::string(next_line, line_end-next_line) : std::string(next_line));
                filtered_text += "\n";
            }
            else if (*next_line && !isspace(next_line[0]))
            {
                filtered_text += "\n"+std::string(next_line);
                break;
            }
        }
    }
    fwrite(filtered_text.data(), filtered_text.size(), 1, stdout);
    exit(0);
}

uint64_t parse_time(std::string time_str, bool *ok)
{
    if (!time_str.length())
    {
        if (ok)
            *ok = false;
        return 0;
    }
    uint64_t mul = 1;
    char type_char = tolower(time_str[time_str.length()-1]);
    if (type_char == 's' || type_char == 'm' || type_char == 'h' || type_char == 'd' || type_char == 'y')
    {
        if (type_char == 's')
            mul = 1;
        else if (time_str[time_str.length()-1] == 'M')
            mul = 30*86400;
        else if (type_char == 'm')
            mul = 60;
        else if (type_char == 'h')
            mul = 3600;
        else if (type_char == 'd')
            mul = 86400;
        else /*if (type_char == 'y')*/
            mul = 86400*365;
        time_str = time_str.substr(0, time_str.length()-1);
    }
    uint64_t ts = stoull_full(time_str, 0) * mul;
    if (ok)
        *ok = !(ts == 0 && time_str != "0" && (time_str != "" || mul != 1));
    return ts;
}

std::string read_all_fd(int fd)
{
    int res_size = 0, res_alloc = 0;
    std::string res;
    while (1)
    {
        if (res_size >= res_alloc)
            res.resize((res_alloc = (res_alloc ? res_alloc*2 : 1024)));
        int r = read(fd, (char*)res.data()+res_size, res_alloc-res_size);
        if (r > 0)
            res_size += r;
        else if (!r || errno != EAGAIN && errno != EINTR)
            break;
    }
    res.resize(res_size);
    return res;
}

std::string str_repeat(const std::string & str, int times)
{
    std::string r;
    for (int i = 0; i < times; i++)
        r += str;
    return r;
}

size_t utf8_length(const std::string & s)
{
    size_t len = 0;
    for (size_t i = 0; i < s.size(); i++)
        len += (s[i] & 0xC0) != 0x80;
    return len;
}

size_t utf8_length(const char *s)
{
    size_t len = 0;
    for (; *s; s++)
        len += (*s & 0xC0) != 0x80;
    return len;
}
