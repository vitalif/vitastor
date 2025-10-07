#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <string.h>
#include <stdio.h>

#include <stdexcept>
#include <set>

#include "addr_util.h"

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr_storage *addr)
{
    if (parse_port)
    {
        int p = str.rfind(':');
        if (p != std::string::npos && (str[0] != '[' || p > 0 && str[p-1] == ']')) // "[ipv6]" which contains ':'
        {
            char null_byte = 0;
            int scanned = sscanf(str.c_str()+p+1, "%d%c", &default_port, &null_byte);
            if (scanned != 1 || default_port >= 0x10000)
                return false;
            str = str.substr(0, p);
        }
    }
    if (inet_pton(AF_INET, str.c_str(), &((struct sockaddr_in*)addr)->sin_addr) == 1)
    {
        addr->ss_family = AF_INET;
        ((struct sockaddr_in*)addr)->sin_port = htons(default_port);
        return true;
    }
    if (str.length() >= 2 && str[0] == '[' && str[str.length()-1] == ']')
        str = str.substr(1, str.length()-2);
    if (inet_pton(AF_INET6, str.c_str(), &((struct sockaddr_in6*)addr)->sin6_addr) == 1)
    {
        addr->ss_family = AF_INET6;
        ((struct sockaddr_in6*)addr)->sin6_port = htons(default_port);
        return true;
    }
    return false;
}

std::string addr_to_string(const sockaddr_storage &addr)
{
    char peer_str[256];
    bool ok = false;
    int port;
    if (addr.ss_family == AF_INET)
    {
        ok = !!inet_ntop(AF_INET, &((sockaddr_in*)&addr)->sin_addr, peer_str, 256);
        port = ntohs(((sockaddr_in*)&addr)->sin_port);
    }
    else if (addr.ss_family == AF_INET6)
    {
        ok = !!inet_ntop(AF_INET6, &((sockaddr_in6*)&addr)->sin6_addr, peer_str, 256);
        port = ntohs(((sockaddr_in6*)&addr)->sin6_port);
    }
    else
        throw std::runtime_error("Unknown address family "+std::to_string(addr.ss_family));
    if (!ok)
        throw std::runtime_error(std::string("inet_ntop: ") + strerror(errno));
    return std::string(peer_str)+":"+std::to_string(port);
}

bool cidr_match(const in_addr &addr, const in_addr &net, uint8_t bits)
{
    if (bits == 0)
    {
        // C99 6.5.7 (3): u32 << 32 is undefined behaviour
        return true;
    }
    return !((addr.s_addr ^ net.s_addr) & htonl(0xFFFFFFFFu << (32 - bits)));
}

bool cidr6_match(const in6_addr &address, const in6_addr &network, uint8_t bits)
{
    const uint32_t *a = address.s6_addr32;
    const uint32_t *n = network.s6_addr32;
    int bits_whole, bits_incomplete;
    bits_whole = bits >> 5;         // number of whole u32
    bits_incomplete = bits & 0x1F;  // number of bits in incomplete u32
    if (bits_whole && memcmp(a, n, bits_whole << 2))
        return false;
    if (bits_incomplete)
    {
        uint32_t mask = htonl((0xFFFFFFFFu) << (32 - bits_incomplete));
        if ((a[bits_whole] ^ n[bits_whole]) & mask)
            return false;
    }
    return true;
}

bool cidr_sockaddr_match(const sockaddr_storage &addr, const addr_mask_t &mask)
{
    return mask.family == addr.ss_family && (mask.family == AF_INET
        ? cidr_match(((sockaddr_in*)&addr)->sin_addr, mask.ipv4, mask.bits)
        : cidr6_match(((sockaddr_in6*)&addr)->sin6_addr, mask.ipv6, mask.bits));
}

addr_mask_t cidr_parse(std::string mask)
{
    unsigned bits = 255;
    int p = mask.find('/');
    if (p != std::string::npos)
    {
        char null_byte = 0;
        if (sscanf(mask.c_str()+p+1, "%u%c", &bits, &null_byte) != 1 || bits > 128)
            throw std::runtime_error("Invalid IP address mask: " + mask);
        mask = mask.substr(0, p);
    }
    in_addr ipv4;
    in6_addr ipv6;
    if (inet_pton(AF_INET, mask.c_str(), &ipv4) == 1)
    {
        if (bits == 255)
            bits = 32;
        if (bits > 32)
            throw std::runtime_error("Invalid IP address mask: " + mask);
        return (addr_mask_t){ .family = AF_INET, .ipv4 = ipv4, .bits = (uint8_t)(bits ? bits : 32) };
    }
    else if (inet_pton(AF_INET6, mask.c_str(), &ipv6) == 1)
    {
        if (bits == 255)
            bits = 128;
        return (addr_mask_t){ .family = AF_INET6, .ipv6 = ipv6, .bits = (uint8_t)bits };
    }
    else
    {
        throw std::runtime_error("Invalid IP address mask: " + mask);
    }
}

std::vector<std::string> getifaddr_list(const std::vector<addr_mask_t> & masks, bool include_v6)
{
    for (auto & mask: masks)
    {
        if (mask.family == AF_INET6)
        {
            // Auto-enable IPv6 addresses
            include_v6 = true;
        }
    }
    std::set<std::string> addresses;
    ifaddrs *list, *ifa;
    if (getifaddrs(&list) == -1)
    {
        throw std::runtime_error(std::string("getifaddrs: ") + strerror(errno));
    }
    for (ifa = list; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (!ifa->ifa_addr)
        {
            continue;
        }
        int family = ifa->ifa_addr->sa_family;
        if ((family == AF_INET || family == AF_INET6 && include_v6) &&
            // Do not skip loopback addresses if the address filter is specified
            (ifa->ifa_flags & (IFF_UP | IFF_RUNNING | (masks.size() ? 0 : IFF_LOOPBACK))) == (IFF_UP | IFF_RUNNING))
        {
            void *addr_ptr;
            if (family == AF_INET)
            {
                addr_ptr = &((sockaddr_in *)ifa->ifa_addr)->sin_addr;
            }
            else
            {
                addr_ptr = &((sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            }
            if (masks.size() > 0)
            {
                int i;
                for (i = 0; i < masks.size(); i++)
                {
                    if (masks[i].family == family && (family == AF_INET
                        ? cidr_match(*(in_addr*)addr_ptr, masks[i].ipv4, masks[i].bits)
                        : cidr6_match(*(in6_addr*)addr_ptr, masks[i].ipv6, masks[i].bits)))
                    {
                        break;
                    }
                }
                if (i >= masks.size())
                {
                    continue;
                }
            }
            char addr[INET6_ADDRSTRLEN];
            if (!inet_ntop(family, addr_ptr, addr, INET6_ADDRSTRLEN))
            {
                throw std::runtime_error(std::string("inet_ntop: ") + strerror(errno));
            }
            addresses.insert(std::string(addr));
        }
    }
    freeifaddrs(list);
    return std::vector<std::string>(addresses.begin(), addresses.end());
}

int create_and_bind_socket(std::string bind_address, int bind_port, int listen_backlog, int *listening_port)
{
    sockaddr_storage addr;
    if (!string_to_addr(bind_address, 0, bind_port, &addr))
    {
        throw std::runtime_error("bind address "+bind_address+" is not valid");
    }

    int listen_fd = socket(addr.ss_family, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }
    int enable = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    }
    if (listening_port)
    {
        if (bind_port == 0)
        {
            socklen_t len = sizeof(addr);
            if (getsockname(listen_fd, (sockaddr *)&addr, &len) == -1)
            {
                close(listen_fd);
                throw std::runtime_error(std::string("getsockname: ") + strerror(errno));
            }
            *listening_port = ntohs(((sockaddr_in*)&addr)->sin_port);
        }
        else
        {
            *listening_port = bind_port;
        }
    }

    if (listen(listen_fd, listen_backlog ? listen_backlog : 128) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("listen: ") + strerror(errno));
    }

    return listen_fd;
}

std::string gethostname_str()
{
    std::string hostname;
    hostname.resize(1024);
    while (gethostname((char*)hostname.data(), hostname.size()) < 0 && errno == ENAMETOOLONG)
        hostname.resize(hostname.size()+1024);
    hostname.resize(strnlen(hostname.data(), hostname.size()));
    return hostname;
}
