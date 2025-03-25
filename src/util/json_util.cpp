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

bool json_is_true(const json11::Json & val)
{
    if (val.is_string())
        return val == "true" || val == "yes" || val == "1";
    return val.bool_value();
}

bool json_is_false(const json11::Json & val)
{
    if (val.is_string())
        return val.string_value() == "false" || val.string_value() == "no" || val.string_value() == "0";
    if (val.is_number())
        return val.number_value() == 0;
    if (val.is_bool())
        return !val.bool_value();
    return false;
}

std::string implode(const std::string & sep, json11::Json array)
{
    if (array.is_number() || array.is_bool() || array.is_string())
    {
        return array.as_string();
    }
    std::string res;
    bool first = true;
    for (auto & item: array.array_items())
    {
        res += (first ? item.as_string() : sep+item.as_string());
        first = false;
    }
    return res;
}
