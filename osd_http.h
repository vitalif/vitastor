#pragma once
#include <string>
#include <vector>
#include <map>
#include "json11/json11.hpp"

struct http_response_t
{
    int status_code;
    std::string status_line;
    std::map<std::string, std::string> headers;
    std::string body;
};

http_response_t *parse_http_response(std::string res);
std::vector<std::string> getifaddr_list(bool include_v6 = false);
uint64_t stoull_full(std::string str, int base = 10);
