#pragma once

#include <netinet/in.h>
#include <sys/socket.h>
#include <string>
#include <vector>

struct addr_mask_t
{
    sa_family_t family;
    in_addr ipv4;
    in6_addr ipv6;
    uint8_t bits;
};

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr_storage *addr);
std::string addr_to_string(const sockaddr_storage &addr);
addr_mask_t cidr_parse(std::string mask);
bool cidr_match(const in_addr &address, const in_addr &network, uint8_t bits);
bool cidr6_match(const in6_addr &address, const in6_addr &network, uint8_t bits);
std::vector<std::string> getifaddr_list(const std::vector<addr_mask_t> & masks = std::vector<addr_mask_t>(), bool include_v6 = false);
int create_and_bind_socket(std::string bind_address, int bind_port, int listen_backlog, int *listening_port);
