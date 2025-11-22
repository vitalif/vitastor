// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

blockstore_i* blockstore_i::create(blockstore_config_t & config, ring_loop_t *ringloop, timerfd_manager_t *tfd)
{
    return new blockstore_impl_t(config, ringloop, tfd);
}
