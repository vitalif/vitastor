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
// AND We must know what we stabilize. Basic workflow is like:
// 1) primary OSD receives sync request
// 2) it determines his own unsynced writes from blockstore's information
//    just before submitting fsync
// 3) it submits syncs to blockstore and peers
// 4) after everyone acks sync it takes the object list and sends stabilize requests to everyone

int blockstore::dequeue_stable(blockstore_operation *op)
{
    obj_ver_id* v;
    int i, todo = 0;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        auto dirty_it = dirty_db.find(*v);
        if (dirty_it == dirty_db.end())
        {
            auto clean_it = clean_db.find(v->oid);
            if (clean_it == clean_db.end() || clean_it->second.version < v->version)
            {
                // No such object version
                op->retval = EINVAL;
                op->callback(op);
                return 1;
            }
            else
            {
                // Already stable
            }
        }
        else if (IS_UNSYNCED(dirty_it->second.state))
        {
            // Object not synced yet. Caller must sync it first
            op->retval = EAGAIN;
            op->callback(op);
            return 1;
        }
        else if (!IS_STABLE(dirty_it->second.state))
        {
            todo++;
        }
    }
    if (!todo)
    {
        // Already stable
        op->retval = 0;
        op->callback(op);
        return 1;
    }
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, todo, sizeof(journal_entry_stable), 0))
    {
        return 0;
    }
    // There is sufficient space. Get SQEs
    struct io_uring_sqe *sqe[space_check.sectors_required];
    for (i = 0; i < space_check.sectors_required; i++)
    {
        BS_SUBMIT_GET_SQE_DECL(sqe[i]);
    }
    // Prepare and submit journal entries
    int s = 0, cur_sector = -1;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        journal_entry_stable *je = (journal_entry_stable*)
            prefill_single_journal_entry(journal, JE_STABLE, sizeof(journal_entry_stable));
        je->oid = v->oid;
        je->version = v->version;
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        if (cur_sector != journal.cur_sector)
        {
            if (cur_sector == -1)
                op->min_used_journal_sector = 1 + journal.cur_sector;
            cur_sector = journal.cur_sector;
            prepare_journal_sector_write(op, journal, sqe[s++]);
        }
    }
    op->max_used_journal_sector = 1 + journal.cur_sector;
    op->pending_ops = s;
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
        // First step: mark dirty_db entries as stable, acknowledge op completion
        obj_ver_id* v;
        int i;
        for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
        {
            // Mark all dirty_db entries up to op->version as stable
            auto dirty_it = dirty_db.find(*v);
            if (dirty_it != dirty_db.end())
            {
                do
                {
                    if (dirty_it->second.state == ST_J_SYNCED)
                    {
                        dirty_it->second.state = ST_J_STABLE;
                    }
                    else if (dirty_it->second.state == ST_D_META_SYNCED)
                    {
                        dirty_it->second.state = ST_D_STABLE;
                    }
                    else if (IS_STABLE(dirty_it->second.state))
                    {
                        break;
                    }
                    dirty_it--;
                } while (dirty_it != dirty_db.begin() && dirty_it->first.oid == v->oid);
                flusher.flush_queue.push_back(*v);
            }
        }
        // Acknowledge op
        op->retval = 0;
        op->callback(op);
    }
}

struct offset_len
{
    uint64_t offset, len;
};

class journal_flusher_t
{
    blockstore *bs;
    int state;
    obj_ver_id cur;
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it;
    std::vector<offset_len> v;
    std::vector<offset_len>::iterator it;
    uint64_t offset, len;

public:
    journal_flusher_t();
    std::deque<obj_ver_id> flush_queue;
    void stabilize_object_loop();
};

#define F_NEXT_OBJ 0
#define F_NEXT_VER 1
#define F_FIND_POS 2
#define F_SUBMIT_FULL 3
#define F_SUBMIT_PART 4
#define F_CUT_OFFSET 5
#define F_FINISH_VER 6

journal_flusher_t::journal_flusher_t()
{
    state = F_NEXT_OBJ;
}

// It would be prettier as a coroutine (maybe https://github.com/hnes/libaco ?)
// Now it's a state machine
void journal_flusher_t::stabilize_object_loop()
{
begin:
    if (state == F_NEXT_OBJ)
    {
        // Pick next object
        if (!flush_queue.size())
            return;
        while (1)
        {
            cur = flush_queue.front();
            flush_queue.pop_front();
            dirty_it = bs->dirty_db.find(cur);
            if (dirty_it != bs->dirty_db.end())
            {
                state = F_NEXT_VER;
                v.clear();
                break;
            }
            else if (flush_queue.size() == 0)
                return;
        }
    }
    if (state == F_NEXT_VER)
    {
        if (dirty_it->second.state == ST_J_STABLE)
        {
            offset = dirty_it->second.offset;
            len = dirty_it->second.size;
            it = v.begin();
            state = F_FIND_POS;
        }
        else if (dirty_it->second.state == ST_D_STABLE)
        {
            
            state = F_NEXT_OBJ;
        }
        else if (IS_STABLE(dirty_it->second.state))
        {
            state = F_NEXT_OBJ;
        }
        else
            state = F_FINISH_VER;
    }
    if (state == F_FIND_POS)
    {
        for (; it != v.end(); it++)
            if (it->offset >= offset)
                break;
        if (it == v.end() || it->offset >= offset+len)
        {
            state = F_SUBMIT_FULL;
        }
        else
        {
            if (it->offset > offset)
                state = F_SUBMIT_PART;
            else
                state = F_CUT_OFFSET;
        }
    }
    if (state == F_SUBMIT_FULL)
    {
        struct io_uring_sqe *sqe = get_sqe();
        if (!sqe)
            return;
        struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
        data->iov = (struct iovec){ malloc(len), len };
        data->op = op; // FIXME OOPS
        io_uring_prep_readv(
            sqe, journal_fd, &data->iov, 1, journal_offset + dirty_it->second.location + offset
        );
        op->pending_ops = 1;
        v.insert(it, (offset_len){ .offset = offset, .len = len });
        state = F_SUBMIT_FULL_WRITE;
        return;
    }
    if (state == F_SUBMIT_FULL_WRITE)
    {
        struct io_uring_sqe *sqe = get_sqe();
        if (!sqe)
            return;
        struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
        
    }
    if (state == F_SUBMIT_PART)
    {
        if (!can_submit)
        {
            return;
        }
        v.insert(it, (offset_len){ .offset = offset, .len = it->offset-offset });
        state = F_CUT_OFFSET;
    }
    if (state == F_CUT_OFFSET)
    {
        if (offset+len > it->offset+it->len)
        {
            len = offset+len - (it->offset+it->len);
            offset = it->offset+it->len;
            state = F_FIND_POS;
        }
        else
            state = F_FINISH_VER;
    }
    if (state == F_FINISH_VER)
    {
        dirty_it--;
        if (dirty_it == bs->dirty_db.begin() || dirty_it->first.oid != cur.oid)
            state = F_NEXT_OBJ;
        else
            state = F_NEXT_VER;
    }
    goto begin;
}
