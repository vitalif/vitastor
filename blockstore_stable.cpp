#include "blockstore.h"

int blockstore::dequeue_stable(blockstore_operation *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    if (dirty_it == dirty_db.end())
    {
        auto clean_it = object_db.find(op->oid);
        if (clean_it == object_db.end() || clean_it->second.version < op->version)
        {
            // No such object version
            op->retval = EINVAL;
        }
        else
        {
            // Already stable
            op->retval = 0;
        }
        op->callback(op);
        return 1;
    }
    else if (IS_UNSYNCED(dirty_it->second.state))
    {
        // Object not synced yet. Caller must sync it first
        op->retval = EAGAIN;
        op->callback(op);
        return 1;
    }
    else if (IS_STABLE(dirty_it->second.state))
    {
        // Already stable
        op->retval = 0;
        op->callback(op);
        return 1;
    }
    return 0;
}

void blockstore::handle_stable_event(ring_data_t *data, blockstore_operation *op)
{
    if (data->res < 0)
    {
        // sync error
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        throw new std::runtime_error("write operation failed. in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111");
    }
    op->pending_ops--;
    if (op->pending_ops == 0)
    {
        
    }
}
