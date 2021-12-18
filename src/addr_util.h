#pragma once

#include <sys/socket.h>
#include <string>

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr *addr);
std::string addr_to_string(const sockaddr &addr);
