// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <map>
#include <string>

#include "json11/json11.hpp"

std::map<std::string, std::string> json_to_string_map(const json11::Json::object & config);
bool json_is_true(const json11::Json & val);
bool json_is_false(const json11::Json & val);
std::string implode(const std::string & sep, json11::Json array);
