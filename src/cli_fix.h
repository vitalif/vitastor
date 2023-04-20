// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "cli.h"

std::vector<uint64_t> parse_uint64_list(json11::Json val);

template<class T> void remove_duplicates(std::vector<T> & ret)
{
    if (!ret.size())
        return;
    std::sort(ret.begin(), ret.end());
    int j = 0;
    for (int i = 1; i < ret.size(); i++)
    {
        if (ret[i] != ret[j])
            ret[++j] = ret[i];
    }
    ret.resize(j+1);
}

// from http_client.cpp...
bool json_is_false(const json11::Json & val);
