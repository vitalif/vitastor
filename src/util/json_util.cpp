// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "json_util.h"

std::map<std::string, std::string> json_to_string_map(const json11::Json::object & config)
{
    std::map<std::string, std::string> bs;
    for (auto kv: config)
    {
        if (kv.second.is_string())
            bs[kv.first] = kv.second.string_value();
        else if (!kv.second.is_null())
            bs[kv.first] = kv.second.dump();
    }
    return bs;
}
