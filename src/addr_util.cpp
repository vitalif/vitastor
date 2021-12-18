#include <arpa/inet.h>
#include <string.h>
#include <stdio.h>

#include <stdexcept>

#include "addr_util.h"

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr *addr)
{
    if (parse_port)
    {
        int p = str.rfind(':');
        if (p != std::string::npos && !(str.length() > 0 && str[p-1] == ']')) // "[ipv6]" which contains ':'
        {
            char null_byte = 0;
            int n = sscanf(str.c_str()+p+1, "%d%c", &default_port, &null_byte);
            if (n != 1 || default_port >= 0x10000)
                return false;
            str = str.substr(0, p);
        }
    }
    if (inet_pton(AF_INET, str.c_str(), &((struct sockaddr_in*)addr)->sin_addr) == 1)
    {
        addr->sa_family = AF_INET;
        ((struct sockaddr_in*)addr)->sin_port = htons(default_port);
        return true;
    }
    if (str.length() >= 2 && str[0] == '[' && str[str.length()-1] == ']')
        str = str.substr(1, str.length()-2);
    if (inet_pton(AF_INET6, str.c_str(), &((struct sockaddr_in6*)addr)->sin6_addr) == 1)
    {
        addr->sa_family = AF_INET6;
        ((struct sockaddr_in6*)addr)->sin6_port = htons(default_port);
        return true;
    }
    return false;
}

std::string addr_to_string(const sockaddr &addr)
{
    char peer_str[256];
    bool ok = false;
    int port;
    if (addr.sa_family == AF_INET)
    {
        ok = !!inet_ntop(AF_INET, &((sockaddr_in*)&addr)->sin_addr, peer_str, 256);
        port = ntohs(((sockaddr_in*)&addr)->sin_port);
    }
    else if (addr.sa_family == AF_INET6)
    {
        ok = !!inet_ntop(AF_INET6, &((sockaddr_in6*)&addr)->sin6_addr, peer_str, 256);
        port = ntohs(((sockaddr_in6*)&addr)->sin6_port);
    }
    else
        throw std::runtime_error("Unknown address family "+std::to_string(addr.sa_family));
    if (!ok)
        throw std::runtime_error(std::string("inet_ntop: ") + strerror(errno));
    return std::string(peer_str)+":"+std::to_string(port);
}
