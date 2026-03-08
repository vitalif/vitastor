// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "cluster_client.h"

void cluster_client_t::vault_destroy()
{
}

void cluster_client_t::vault_load_keys()
{
    if (vault_loading || !vault_key_load_queue.size())
    {
        return;
    }
    vault_loading = true;
}
