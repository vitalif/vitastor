// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once
#include <stdint.h>
#include <string>

std::string base64_encode(const std::string &in);
std::string base64_decode(const std::string &in);
uint64_t parse_size(std::string size_str);
std::string strtolower(const std::string & in);
std::string trim(const std::string & in, const char *rm_chars = " \n\r\t");
std::string str_replace(const std::string & in, const std::string & needle, const std::string & replacement);
uint64_t stoull_full(const std::string & str, int base = 0);
std::string format_size(uint64_t size, bool nobytes = false);
void print_help(const char *help_text, std::string exe_name, std::string cmd, bool all);
