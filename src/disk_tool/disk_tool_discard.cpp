// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "str_util.h"
#include "json_util.h"

int disk_tool_t::trim_data(std::string device)
{
    int r;
    // Parse parameters
    if (stoull_full(device))
        device = "/dev/vitastor/osd"+device+"-data";
    json11::Json sb = read_osd_superblock(device, true, false);
    if (sb.is_null())
        return 1;
    auto sb_params = json_to_string_map(sb["params"].object_items());
    if (options["discard_granularity"] != "")
        sb_params["discard_granularity"] = options["discard_granularity"];
    if (options["min_discard_size"] != "")
        sb_params["min_discard_size"] = options["min_discard_size"];
    try
    {
        dsk.parse_config(sb_params);
    }
    catch (std::exception & e)
    {
        fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
    if (!dsk.discard_granularity && sb["real_data_device"].string_value().substr(0, 5) == "/dev/")
    {
        auto dg = read_file("/sys/block/"+sb["real_data_device"].string_value().substr(5)+"/queue/discard_granularity", true);
        if (dg != "")
            dsk.discard_granularity = parse_size(trim(dg));
    }
    // Open devices
    try
    {
        if (options["io"] != "")
            dsk.data_io = dsk.meta_io = dsk.journal_io = options["io"];
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths();
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
    // Fill allocator
    fprintf(stderr, "Reading metadata\n");
    data_alloc = new allocator_t(dsk.block_count);
    r = process_meta(
        [this](blockstore_meta_header_v2_t *hdr) {},
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            data_alloc->set(block_num, true);
        },
        false
    );
    if (r != 0)
    {
        dsk.close_all();
        return r;
    }
    fprintf(stderr, "Reading journal\n");
    r = process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
            {
                data_alloc->set(je->big_write.location / dsk.data_block_size, true);
            }
        });
    }, false);
    if (r != 0)
    {
        dsk.close_all();
        return r;
    }
    // Trim
    r = dsk.trim_data([&](uint64_t block_num) { return data_alloc->get(block_num); });
    dsk.close_all();
    return r == 0;
}
