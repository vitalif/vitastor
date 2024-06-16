// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once
#include <stdint.h>
#include <string>
#include <vector>

#define is_white(a) ((a) == ' ' || (a) == '\t' || (a) == '\r' || (a) == '\n')

std::string base64_encode(const std::string &in);
std::string base64_decode(const std::string &in);
uint64_t parse_size(std::string size_str, bool *ok = NULL);
std::string strtoupper(const std::string & in);
std::string strtolower(const std::string & in);
std::string trim(const std::string & in, const char *rm_chars = " \n\r\t");
std::string str_replace(const std::string & in, const std::string & needle, const std::string & replacement);
uint64_t stoull_full(const std::string & str, int base = 0);
std::string format_size(uint64_t size, bool nobytes = false, bool nospace = false);
void print_help(const char *help_text, std::string exe_name, std::string cmd, bool all);
uint64_t parse_time(std::string time_str, bool *ok = NULL);
std::string read_all_fd(int fd);
std::string read_file(std::string file, bool allow_enoent = false);
std::string str_repeat(const std::string & str, int times);
size_t utf8_length(const std::string & s);
size_t utf8_length(const char *s);
std::vector<std::string> explode(const std::string & sep, const std::string & value, bool trim);
std::string scan_escaped(const char *cmd, size_t size, size_t & pos, bool allow_unquoted = true);
std::string scan_escaped(const std::string & cmd, size_t & pos, bool allow_unquoted = true);
std::string auto_addslashes(const std::string & str, const char *toescape = "\\\"");
std::string addslashes(const std::string & str, const char *toescape = "\\\"");
std::string realpath_str(std::string path, bool nofail = true);
