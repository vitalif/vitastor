#include "base64.h"

std::string base64_encode(const std::string &in)
{
    std::string out;
    unsigned val = 0, valb = -6;
    for (unsigned char c: in)
    {
        val = (val<<8) + c;
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
    unsigned val = 0, valb = -8;
    for (unsigned char c: in)
    {
        if (T[c] == -1)
            break;
        val = (val<<6) + T[c];
        valb += 6;
        if (valb>=0)
        {
            out.push_back(char((val>>valb)&0xFF));
            valb -= 8;
        }
    }
    return out;
}
