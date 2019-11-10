#include "blockstore.h"

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
    // FIXME: Try to deduplicate journal entry submission code...
    // Check journal space
    uint64_t next_pos = journal.next_free;
    if (512 - journal.in_sector_pos < sizeof(struct journal_entry_stable))
    {
        next_pos = (next_pos+512) < journal.len ? next_pos+512 : 512;
        // Also check if we have an unused memory buffer for the journal sector
        if (journal.sector_info[((journal.cur_sector + 1) % journal.sector_count)].usage_count > 0)
        {
            // No memory buffer available. Wait for it.
            op->wait_for = WAIT_JOURNAL_BUFFER;
            return 0;
        }
    }
    if (next_pos >= journal.used_start)
    {
        // No space in the journal. Wait for it.
        op->wait_for = WAIT_JOURNAL;
        op->wait_detail = next_pos;
        return 0;
    }
    // There is sufficient space. Get SQE
    BS_SUBMIT_GET_SQE(sqe, data);
    // Got SQE. Prepare journal sector write
    if (512 - journal.in_sector_pos < sizeof(struct journal_entry_stable))
    {
        // Move to the next journal sector
        // Also select next sector buffer in memory
        journal.cur_sector = ((journal.cur_sector + 1) % journal.sector_count);
        journal.sector_info[journal.cur_sector].offset = journal.next_free;
        journal.in_sector_pos = 0;
        journal.next_free = (journal.next_free+512) < journal.len ? journal.next_free + 512 : 512;
        memset(journal.sector_buf + 512*journal.cur_sector, 0, 512);
    }
    journal_entry_stable *je = (journal_entry_stable*)(
        journal.sector_buf + 512*journal.cur_sector + journal.in_sector_pos
    );
    *je = {
        .crc32 = 0,
        .magic = JOURNAL_MAGIC,
        .type = JE_STABLE,
        .size = sizeof(struct journal_entry_stable),
        .crc32_prev = journal.crc32_last,
        .oid = op->oid,
        .version = op->version,
    };
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
        // Mark dirty_db entry as stable
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        if (dirty_it->second.state == ST_J_SYNCED)
        {
            dirty_it->second.state = ST_J_STABLE;
            // Copy data from the journal to the data device
            // -> increase version on the metadata device
            // -> advance clean_db entry's version and clear previous journal entries
            // This makes 1 4K small write look like:
            // 512b+4K (journal) + sync + 512b (journal) + sync + 512b (metadata) + 4K (data) + sync.
            // WA = 2.375. It's not the best, SSD FTL-like redirect-write with defragmentation
            // could probably be lower even with defragmentation. But it's fixed and it's still
            // better than in Ceph. :)
        }
        else if (dirty_it->second.state == ST_D_META_SYNCED)
        {
            dirty_it->second.state = ST_D_STABLE;
            // Copy metadata from the journal to the metadata device
            // -> move dirty_db entry to clean_db and clear previous journal entries
            // This makes 1 128K big write look like:
            // 128K (data) + sync + 512b (journal) + sync + 512b (journal) + sync + 512b (metadata) + sync.
            // WA = 1.012. Very good :)
        }
        
    }
}
