// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <map>
#include <string>

#include "json11/json11.hpp"

std::map<std::string, std::string> json_to_string_map(const json11::Json::object & config);
