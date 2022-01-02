#pragma once

#include <sys/socket.h>
#include <string>
#include <vector>

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr *addr);
std::string addr_to_string(const sockaddr &addr);
std::vector<std::string> getifaddr_list(std::vector<std::string> mask_cfg = std::vector<std::string>(), bool include_v6 = false);
