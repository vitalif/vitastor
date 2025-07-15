// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "str_util.h"

#include "blockstore_impl.h"
#include "v1/impl.h"

blockstore_i* blockstore_i::create(blockstore_config_t & config, ring_loop_i *ringloop, timerfd_manager_t *tfd)
{
    auto meta_format = stoull_full(config["meta_format"]);
    if (meta_format == BLOCKSTORE_META_FORMAT_HEAP)
        return new blockstore_impl_t(config, ringloop, tfd);
    else
        return new v1::blockstore_impl_t(config, ringloop, tfd);
}
