#include "blockstore.h"

// Stabilize small write:
// 1) Copy data from the journal to the data device
//    Sync it before writing metadata if we want to keep metadata consistent
//    Overall it's optional because it can be replayed from the journal until
//    it's cleared, and reads are also fulfilled from the journal
// 2) Increase version on the metadata device and sync it
// 3) Advance clean_db entry's version, clear previous journal entries
//
// This makes 1 4K small write+sync look like:
// 512b+4K (journal) + sync + 512b (journal) + sync + 4K (data) [+ sync?] + 512b (metadata) + sync.
// WA = 2.375. It's not the best, SSD FTL-like redirect-write with defragmentation
// could probably be lower even with defragmentation. But it's fixed and it's still
// better than in Ceph. :)

// Stabilize big write:
// 1) Copy metadata from the journal to the metadata device
// 2) Move dirty_db entry to clean_db and clear previous journal entries
//
// This makes 1 128K big write+sync look like:
// 128K (data) + sync + 512b (journal) + sync + 512b (journal) + sync + 512b (metadata) + sync.
// WA = 1.012. Very good :)

// AND We must do it in batches, for the sake of reduced fsync call count

int blockstore::dequeue_stable(blockstore_operation *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    if (dirty_it == dirty_db.end())
    {
        auto clean_it = clean_db.find(op->oid);
        if (clean_it == clean_db.end() || clean_it->second.version < op->version)
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
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_stable), 0))
    {
        return 0;
    }
    // There is sufficient space. Get SQE
    BS_SUBMIT_GET_SQE(sqe, data);
    // Got SQE. Prepare journal sector write
    journal_entry_stable *je = (journal_entry_stable*)prefill_single_journal_entry(journal, JE_STABLE, sizeof(struct journal_entry_stable));
    je->oid = op->oid;
    je->version = op->version;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    data->iov = (struct iovec){ journal.sector_buf + 512*journal.cur_sector, 512 };
    data->op = op;
    io_uring_prep_writev(
        sqe, journal.fd, &data->iov, 1, journal.offset + journal.sector_info[journal.cur_sector].offset
    );
    journal.sector_info[journal.cur_sector].usage_count++;
    op->pending_ops = 1;
    op->min_used_journal_sector = op->max_used_journal_sector = 1 + journal.cur_sector;
    return 1;
}

int blockstore::continue_stable(blockstore_operation *op)
{
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
        // Mark all dirty_db entries up to op->version as stable
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        if (dirty_it->second.state == ST_J_SYNCED)
        {
            dirty_it->second.state = ST_J_STABLE;
            // Acknowledge op
            op->retval = 0;
            op->callback(op);
        }
        else if (dirty_it->second.state == ST_D_META_SYNCED)
        {
            dirty_it->second.state = ST_D_STABLE;
            // Acknowledge op
            op->retval = 0;
            op->callback(op);
        }
        else if (dirty_it->second.state == ST_J_STABLE)
        {
            
        }
        else if (dirty_it->second.state == ST_D_STABLE)
        {
            
        }
    }
}
