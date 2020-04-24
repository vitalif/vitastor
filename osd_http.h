#pragma once
#include <string>
#include <vector>
#include <map>
#include "json11/json11.hpp"

struct http_response_t
{
    bool eof = false;
    int error_code = 0;
    int status_code = 0;
    std::string status_line;
    std::map<std::string, std::string> headers;
    std::string body;
};

void parse_headers(std::string & res, http_response_t *parsed);
std::vector<std::string> getifaddr_list(bool include_v6 = false);
uint64_t stoull_full(const std::string & str, int base = 10);
