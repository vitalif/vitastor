#include "blockstore.h"

blockstore_journal_check_t::blockstore_journal_check_t(blockstore *bs)
{
    this->bs = bs;
    sectors_required = 0;
    next_pos = bs->journal.next_free;
    next_sector = bs->journal.cur_sector;
    next_in_pos = bs->journal.in_sector_pos;
}

// Check if we can write <required> entries of <size> bytes and <data_after> data bytes after them to the journal
int blockstore_journal_check_t::check_available(blockstore_operation *op, int required, int size, int data_after)
{
    while (1)
    {
        int fits = (512 - next_in_pos) / size;
        if (fits > 0)
        {
            required -= fits;
            next_in_pos += fits * size;
            sectors_required++;
        }
        if (required <= 0)
            break;
        next_pos = (next_pos+512) < bs->journal.len ? next_pos+512 : 512;
        next_sector = ((next_sector + 1) % bs->journal.sector_count);
        next_in_pos = 0;
        if (bs->journal.sector_info[next_sector].usage_count > 0)
        {
            // No memory buffer available. Wait for it.
            op->wait_for = WAIT_JOURNAL_BUFFER;
            return 0;
        }
    }
    if (data_after > 0)
    {
        next_pos = (bs->journal.len - next_pos < data_after ? 512 : next_pos) + data_after;
    }
    if (next_pos >= bs->journal.used_start)
    {
        // No space in the journal. Wait for it.
        op->wait_for = WAIT_JOURNAL;
        op->wait_detail = next_pos;
        return 0;
    }
    return 1;
}

void prepare_journal_sector_write(journal_t & journal, io_uring_sqe *sqe, std::function<void(ring_data_t*)> cb)
{
    journal.sector_info[journal.cur_sector].usage_count++;
    ring_data_t *data = ((ring_data_t*)sqe->user_data);
    data->iov = (struct iovec){ journal.sector_buf + 512*journal.cur_sector, 512 };
    data->callback = cb;
    my_uring_prep_writev(
        sqe, journal.fd, &data->iov, 1, journal.offset + journal.sector_info[journal.cur_sector].offset
    );
}
