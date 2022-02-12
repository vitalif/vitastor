#pragma once

#include <sys/socket.h>
#include <string>
#include <vector>

bool string_to_addr(std::string str, bool parse_port, int default_port, struct sockaddr_storage *addr);
std::string addr_to_string(const sockaddr_storage &addr);
std::vector<std::string> getifaddr_list(std::vector<std::string> mask_cfg = std::vector<std::string>(), bool include_v6 = false);
int create_and_bind_socket(std::string bind_address, int bind_port, int listen_backlog, int *listening_port);
