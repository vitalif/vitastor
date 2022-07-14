// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <assert.h>
#include "base64.h"

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

uint64_t parse_size(std::string size_str)
{
    if (!size_str.length())
    {
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
    if (size == 0 && size_str != "0" && (size_str != "" || mul != 1))
    {
        return UINT64_MAX;
    }
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
