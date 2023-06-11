// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

#define META_BLOCK_UNREAD 0
#define META_BLOCK_READ 1

journal_flusher_t::journal_flusher_t(blockstore_impl_t *bs)
{
    this->bs = bs;
    this->max_flusher_count = bs->max_flusher_count;
    this->min_flusher_count = bs->min_flusher_count;
    this->cur_flusher_count = bs->min_flusher_count;
    this->target_flusher_count = bs->min_flusher_count;
    dequeuing = false;
    trimming = false;
    active_flushers = 0;
    syncing_flushers = 0;
    // FIXME: allow to configure flusher_start_threshold and journal_trim_interval
    flusher_start_threshold = bs->dsk.journal_block_size / sizeof(journal_entry_stable);
    journal_trim_interval = 512;
    journal_trim_counter = bs->journal.flush_journal ? 1 : 0;
    trim_wanted = bs->journal.flush_journal ? 1 : 0;
    journal_superblock = bs->journal.inmemory ? bs->journal.buffer : memalign_or_die(MEM_ALIGNMENT, bs->dsk.journal_block_size);
    co = new journal_flusher_co[max_flusher_count];
    for (int i = 0; i < max_flusher_count; i++)
    {
        co[i].bs = bs;
        co[i].flusher = this;
    }
}

journal_flusher_co::journal_flusher_co()
{
    wait_state = 0;
    simple_callback_r = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
            bs->disk_error_abort("read operation during flush", data->res, data->iov.iov_len);
        wait_count--;
    };
    simple_callback_rj = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
            bs->disk_error_abort("read operation during flush", data->res, data->iov.iov_len);
        wait_journal_count--;
    };
    simple_callback_w = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
            bs->disk_error_abort("write operation during flush", data->res, data->iov.iov_len);
        wait_count--;
    };
}

journal_flusher_t::~journal_flusher_t()
{
    if (!bs->journal.inmemory)
        free(journal_superblock);
    delete[] co;
}

bool journal_flusher_t::is_active()
{
    return active_flushers > 0 || dequeuing;
}

void journal_flusher_t::loop()
{
    target_flusher_count = bs->write_iodepth*2;
    if (target_flusher_count < min_flusher_count)
        target_flusher_count = min_flusher_count;
    else if (target_flusher_count > max_flusher_count)
        target_flusher_count = max_flusher_count;
    if (target_flusher_count > cur_flusher_count)
        cur_flusher_count = target_flusher_count;
    else if (target_flusher_count < cur_flusher_count)
    {
        while (target_flusher_count < cur_flusher_count)
        {
            if (co[cur_flusher_count-1].wait_state)
                break;
            cur_flusher_count--;
        }
    }
    for (int i = 0; (active_flushers > 0 || dequeuing || trim_wanted > 0) && i < cur_flusher_count; i++)
        co[i].loop();
}

void journal_flusher_t::enqueue_flush(obj_ver_id ov)
{
#ifdef BLOCKSTORE_DEBUG
    printf("enqueue_flush %lx:%lx v%lu\n", ov.oid.inode, ov.oid.stripe, ov.version);
#endif
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        if (it->second < ov.version)
            it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        flush_queue.push_back(ov.oid);
    }
    if (!dequeuing && (flush_queue.size() >= flusher_start_threshold || trim_wanted > 0))
    {
        dequeuing = true;
        bs->ringloop->wakeup();
    }
}

void journal_flusher_t::unshift_flush(obj_ver_id ov, bool force)
{
#ifdef BLOCKSTORE_DEBUG
    printf("unshift_flush %lx:%lx v%lu\n", ov.oid.inode, ov.oid.stripe, ov.version);
#endif
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        if (it->second < ov.version)
            it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        if (!force)
            flush_queue.push_front(ov.oid);
    }
    if (force)
        flush_queue.push_front(ov.oid);
    if (force || !dequeuing && (flush_queue.size() >= flusher_start_threshold || trim_wanted > 0))
    {
        dequeuing = true;
        bs->ringloop->wakeup();
    }
}

void journal_flusher_t::remove_flush(object_id oid)
{
#ifdef BLOCKSTORE_DEBUG
    printf("undo_flush %lx:%lx\n", oid.inode, oid.stripe);
#endif
    auto v_it = flush_versions.find(oid);
    if (v_it != flush_versions.end())
    {
        flush_versions.erase(v_it);
        for (auto q_it = flush_queue.begin(); q_it != flush_queue.end(); q_it++)
        {
            if (*q_it == oid)
            {
                flush_queue.erase(q_it);
                break;
            }
        }
    }
}

void journal_flusher_t::request_trim()
{
    dequeuing = true;
    trim_wanted++;
    bs->ringloop->wakeup();
}

void journal_flusher_t::mark_trim_possible()
{
    if (trim_wanted > 0)
    {
        dequeuing = true;
        if (!journal_trim_counter)
            journal_trim_counter = journal_trim_interval;
        bs->ringloop->wakeup();
    }
}

void journal_flusher_t::release_trim()
{
    trim_wanted--;
}

void journal_flusher_t::dump_diagnostics()
{
    const char *unflushable_type = "";
    obj_ver_id unflushable = {};
    // Try to find out if there is a flushable object for information
    for (object_id cur_oid: flush_queue)
    {
        obj_ver_id cur = { .oid = cur_oid, .version = flush_versions[cur_oid] };
        auto dirty_end = bs->dirty_db.find(cur);
        if (dirty_end == bs->dirty_db.end())
        {
            // Already flushed
            continue;
        }
        auto repeat_it = sync_to_repeat.find(cur.oid);
        if (repeat_it != sync_to_repeat.end())
        {
            // Someone is already flushing it
            unflushable_type = "locked,";
            unflushable = cur;
            break;
        }
        if (dirty_end->second.journal_sector >= bs->journal.dirty_start &&
            (bs->journal.dirty_start >= bs->journal.used_start ||
            dirty_end->second.journal_sector < bs->journal.used_start))
        {
            // Object is more recent than possible to flush
            bool found = try_find_older(dirty_end, cur);
            if (!found)
            {
                unflushable_type = "dirty,";
                unflushable = cur;
                break;
            }
        }
        unflushable_type = "ok,";
        unflushable = cur;
        break;
    }
    printf(
        "Flusher: queued=%ld first=%s%lx:%lx trim_wanted=%d dequeuing=%d trimming=%d cur=%d target=%d active=%d syncing=%d\n",
        flush_queue.size(), unflushable_type, unflushable.oid.inode, unflushable.oid.stripe,
        trim_wanted, dequeuing, trimming, cur_flusher_count, target_flusher_count,
        active_flushers, syncing_flushers
    );
}

bool journal_flusher_t::try_find_older(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur)
{
    bool found = false;
    while (dirty_end != bs->dirty_db.begin())
    {
        dirty_end--;
        if (dirty_end->first.oid != cur.oid)
        {
            break;
        }
        if (!(dirty_end->second.journal_sector >= bs->journal.dirty_start &&
            (bs->journal.dirty_start >= bs->journal.used_start ||
            dirty_end->second.journal_sector < bs->journal.used_start)))
        {
            found = true;
            cur.version = dirty_end->first.version;
            break;
        }
    }
    return found;
}

bool journal_flusher_t::try_find_other(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur)
{
    int search_left = flush_queue.size() - 1;
#ifdef BLOCKSTORE_DEBUG
    printf("Flusher overran writers (%lx:%lx v%lu, dirty_start=%08lx) - searching for older flushes (%d left)\n",
        cur.oid.inode, cur.oid.stripe, cur.version, bs->journal.dirty_start, search_left);
#endif
    while (search_left > 0)
    {
        cur.oid = flush_queue.front();
        cur.version = flush_versions[cur.oid];
        flush_queue.pop_front();
        flush_versions.erase(cur.oid);
        dirty_end = bs->dirty_db.find(cur);
        if (dirty_end != bs->dirty_db.end())
        {
            if (dirty_end->second.journal_sector >= bs->journal.dirty_start &&
                (bs->journal.dirty_start >= bs->journal.used_start ||
                dirty_end->second.journal_sector < bs->journal.used_start))
            {
#ifdef BLOCKSTORE_DEBUG
                printf("Write %lx:%lx v%lu is too new: offset=%08lx\n", cur.oid.inode, cur.oid.stripe, cur.version, dirty_end->second.journal_sector);
#endif
                enqueue_flush(cur);
            }
            else
            {
                auto repeat_it = sync_to_repeat.find(cur.oid);
                if (repeat_it != sync_to_repeat.end())
                {
                    if (repeat_it->second < cur.version)
                        repeat_it->second = cur.version;
                }
                else
                {
                    sync_to_repeat[cur.oid] = 0;
                    break;
                }
            }
        }
        search_left--;
    }
    if (search_left <= 0)
    {
#ifdef BLOCKSTORE_DEBUG
        printf("No older flushes, stopping\n");
#endif
    }
    return search_left > 0;
}

#define await_sqe(label) \
    resume_##label:\
        sqe = bs->get_sqe();\
        if (!sqe)\
        {\
            wait_state = wait_base+label;\
            return false;\
        }\
        data = ((ring_data_t*)sqe->user_data);

bool journal_flusher_co::loop()
{
    int wait_base = 0;
    // This is much better than implementing the whole function as an FSM
    // Maybe I should consider a coroutine library like https://github.com/hnes/libaco ...
    // Or just C++ coroutines, but they require some wrappers
    if (wait_state == 1)       goto resume_1;
    else if (wait_state == 2)  goto resume_2;
    else if (wait_state == 3)  goto resume_3;
    else if (wait_state == 4)  goto resume_4;
    else if (wait_state == 5)  goto resume_5;
    else if (wait_state == 6)  goto resume_6;
    else if (wait_state == 7)  goto resume_7;
    else if (wait_state == 8)  goto resume_8;
    else if (wait_state == 9)  goto resume_9;
    else if (wait_state == 10) goto resume_10;
    else if (wait_state == 11) goto resume_11;
    else if (wait_state == 12) goto resume_12;
    else if (wait_state == 13) goto resume_13;
    else if (wait_state == 14) goto resume_14;
    else if (wait_state == 15) goto resume_15;
    else if (wait_state == 16) goto resume_16;
    else if (wait_state == 17) goto resume_17;
    else if (wait_state == 18) goto resume_18;
    else if (wait_state == 19) goto resume_19;
    else if (wait_state == 20) goto resume_20;
    else if (wait_state == 21) goto resume_21;
    else if (wait_state == 22) goto resume_22;
    else if (wait_state == 23) goto resume_23;
    else if (wait_state == 24) goto resume_24;
    else if (wait_state == 25) goto resume_25;
    else if (wait_state == 26) goto resume_26;
    else if (wait_state == 27) goto resume_27;
    else if (wait_state == 28) goto resume_28;
    else if (wait_state == 29) goto resume_29;
resume_0:
    if (flusher->flush_queue.size() < flusher->min_flusher_count && !flusher->trim_wanted ||
        !flusher->flush_queue.size() || !flusher->dequeuing)
    {
stop_flusher:
        if (flusher->trim_wanted > 0 && flusher->journal_trim_counter > 0)
        {
            // Attempt forced trim
            flusher->active_flushers++;
            goto trim_journal;
        }
        flusher->dequeuing = false;
        wait_state = 0;
        return true;
    }
    cur.oid = flusher->flush_queue.front();
    cur.version = flusher->flush_versions[cur.oid];
    flusher->flush_queue.pop_front();
    flusher->flush_versions.erase(cur.oid);
    dirty_end = bs->dirty_db.find(cur);
    if (dirty_end != bs->dirty_db.end())
    {
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it != flusher->sync_to_repeat.end())
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Postpone %lx:%lx v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
            // We don't flush different parts of history of the same object in parallel
            // So we check if someone is already flushing this object
            // In that case we set sync_to_repeat and pick another object
            // Another coroutine will see it and re-queue the object after it finishes
            if (repeat_it->second < cur.version)
                repeat_it->second = cur.version;
            wait_state = 0;
            goto resume_0;
        }
        else
            flusher->sync_to_repeat[cur.oid] = 0;
        if (dirty_end->second.journal_sector >= bs->journal.dirty_start &&
            (bs->journal.dirty_start >= bs->journal.used_start ||
            dirty_end->second.journal_sector < bs->journal.used_start))
        {
            flusher->enqueue_flush(cur);
            // We can't flush journal sectors that are still written to
            // However, as we group flushes by oid, current oid may have older writes to flush!
            // And it may even block writes if we don't flush the older version
            // (if it's in the beginning of the journal)...
            // So first try to find an older version of the same object to flush.
            if (!flusher->try_find_older(dirty_end, cur))
            {
                // Try other objects
                flusher->sync_to_repeat.erase(cur.oid);
                if (!flusher->try_find_other(dirty_end, cur))
                {
                    goto stop_flusher;
                }
            }
        }
#ifdef BLOCKSTORE_DEBUG
        printf("Flushing %lx:%lx v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
        flusher->active_flushers++;
        // Find it in clean_db
        {
            auto & clean_db = bs->clean_db_shard(cur.oid);
            auto clean_it = clean_db.find(cur.oid);
            old_clean_ver = (clean_it != clean_db.end() ? clean_it->second.version : 0);
            old_clean_loc = (clean_it != clean_db.end() ? clean_it->second.location : UINT64_MAX);
        }
        // Scan dirty versions of the object to determine what we need to read
        scan_dirty();
        // Writes and deletes shouldn't happen at the same time
        assert(!has_writes || !has_delete);
        if (!has_writes && !has_delete || has_delete && old_clean_loc == UINT64_MAX)
        {
            // Nothing to flush
            bs->erase_dirty(dirty_start, std::next(dirty_end), clean_loc);
            goto release_oid;
        }
        if (clean_loc == UINT64_MAX)
        {
            if (old_clean_loc == UINT64_MAX)
            {
                // Object not allocated. This is a bug.
                char err[1024];
                snprintf(
                    err, 1024, "BUG: Object %lx:%lx v%lu that we are trying to flush is not allocated on the data device",
                    cur.oid.inode, cur.oid.stripe, cur.version
                );
                throw std::runtime_error(err);
            }
            else
            {
                clean_loc = old_clean_loc;
            }
        }
        // Submit dirty data and old checksum data reads
resume_1:
resume_2:
        if (!read_dirty(1))
            return false;
        // Also we may need to read metadata. We do read-modify-write cycle(s) for every operation.
    resume_3:
    resume_4:
        if (!modify_meta_do_reads(3))
            return false;
        // Now, if csum_block_size is > bitmap_granularity and if we are doing partial checksum block updates,
        // perform a trick: clear bitmap bits in the metadata entry and recalculate block checksum with zeros
        // in place of overwritten parts. Then, even if the actual partial update fully or partially fails,
        // we'll have a correct checksum because it won't include overwritten parts!
        // The same thing actually happens even when csum_block_size == bitmap_granularity, but in that case
        // we never need to read (and thus verify) overwritten parts from the data device.
    resume_5:
    resume_6:
    resume_7:
    resume_8:
    resume_9:
    resume_10:
    resume_11:
        if (fill_incomplete && !clear_incomplete_csum_block_bits(5))
            return false;
        // Wait for journal data reads if the journal is not inmemory
    resume_12:
        if (wait_journal_count > 0)
        {
            wait_state = wait_base+12;
            return false;
        }
        // Submit data writes
        for (it = v.begin(); it != v.end(); it++)
        {
            if (it->copy_flags == COPY_BUF_JOURNAL || it->copy_flags == (COPY_BUF_JOURNAL|COPY_BUF_COALESCED))
            {
                await_sqe(13);
                data->iov = (struct iovec){ it->buf, (size_t)it->len };
                data->callback = simple_callback_w;
                my_uring_prep_writev(
                    sqe, bs->dsk.data_fd, &data->iov, 1, bs->dsk.data_offset + clean_loc + it->offset
                );
                wait_count++;
            }
        }
        // Wait for data writes and metadata reads
    resume_14:
    resume_15:
        if (!wait_meta_reads(14))
            return false;
        // Sync data before writing metadata
    resume_16:
    resume_17:
    resume_18:
        if (copy_count && !fsync_batch(false, 16))
            return false;
        // Modify the new metadata entry
        update_metadata_entry();
        // Update clean_db - it must be equal to the metadata entry
        update_clean_db();
        // And write metadata entries
        if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
        {
            // zero out old metadata entry
            {
                clean_disk_entry *old_entry = (clean_disk_entry*)((uint8_t*)meta_old.buf + meta_old.pos*bs->dsk.clean_entry_size);
                if (old_entry->oid.inode != 0 && old_entry->oid != cur.oid)
                {
                    printf("Fatal error (metadata corruption or bug): tried to wipe metadata entry %lu (%lx:%lx v%lu) as old location of %lx:%lx\n",
                        old_clean_loc >> bs->dsk.block_order, old_entry->oid.inode, old_entry->oid.stripe,
                        old_entry->version, cur.oid.inode, cur.oid.stripe);
                    exit(1);
                }
            }
            memset((uint8_t*)meta_old.buf + meta_old.pos*bs->dsk.clean_entry_size, 0, bs->dsk.clean_entry_size);
    resume_19:
            if (meta_old.sector != meta_new.sector && !write_meta_block(meta_old, 19))
                return false;
        }
    resume_20:
        if (!write_meta_block(meta_new, 20))
            return false;
    resume_21:
        if (wait_count > 0)
        {
            wait_state = wait_base+21;
            return false;
        }
        // Done, free all buffers
        free_buffers();
        // And sync metadata (in batches - not per each operation!)
    resume_22:
    resume_23:
    resume_24:
        if (!fsync_batch(true, 22))
            return false;
        // Free the data block only when metadata is synced
        free_data_blocks();
        // Erase dirty_db entries
        bs->erase_dirty(dirty_start, std::next(dirty_end), clean_loc);
#ifdef BLOCKSTORE_DEBUG
        printf("Flushed %lx:%lx v%lu (%d copies, wr:%d, del:%d), %ld left\n", cur.oid.inode, cur.oid.stripe, cur.version,
            copy_count, has_writes, has_delete, flusher->flush_queue.size());
#endif
    release_oid:
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it != flusher->sync_to_repeat.end() && repeat_it->second > cur.version)
        {
            // Requeue version
            flusher->unshift_flush({ .oid = cur.oid, .version = repeat_it->second }, false);
        }
        flusher->sync_to_repeat.erase(repeat_it);
    trim_journal:
        // Clear unused part of the journal every <journal_trim_interval> flushes
        if (!((++flusher->journal_trim_counter) % flusher->journal_trim_interval) || flusher->trim_wanted > 0)
        {
    resume_25:
    resume_26:
    resume_27:
    resume_28:
    resume_29:
            if (!trim_journal(25))
                return false;
        }
        // All done
        flusher->active_flushers--;
        wait_state = 0;
        goto resume_0;
    }
    return true;
}

void journal_flusher_co::update_metadata_entry()
{
    clean_disk_entry *new_entry = (clean_disk_entry*)((uint8_t*)meta_new.buf + meta_new.pos*bs->dsk.clean_entry_size);
    if (new_entry->oid.inode != 0 && new_entry->oid != cur.oid)
    {
        printf(
            has_delete
                ? "Fatal error (metadata corruption or bug): tried to delete metadata entry %lu (%lx:%lx v%lu) while deleting %lx:%lx v%lu\n"
                : "Fatal error (metadata corruption or bug): tried to overwrite non-zero metadata entry %lu (%lx:%lx v%lu) with %lx:%lx v%lu\n",
            clean_loc >> bs->dsk.block_order, new_entry->oid.inode, new_entry->oid.stripe,
            new_entry->version, cur.oid.inode, cur.oid.stripe, cur.version
        );
        exit(1);
    }
    if (has_delete)
    {
        // Zero out the new metadata entry
        memset((uint8_t*)meta_new.buf + meta_new.pos*bs->dsk.clean_entry_size, 0, bs->dsk.clean_entry_size);
    }
    else
    {
        if (bs->dsk.csum_block_size)
        {
            // Copy the whole old metadata entry before updating it if the updated object is being read
            obj_ver_id ov = { .oid = new_entry->oid, .version = new_entry->version };
            auto uo_it = bs->used_clean_objects.upper_bound(ov);
            if (uo_it != bs->used_clean_objects.begin())
            {
                uo_it--;
                if (uo_it->first.oid == new_entry->oid)
                {
                    uint8_t *meta_copy = (uint8_t*)malloc_or_die(bs->dsk.clean_entry_size);
                    memcpy(meta_copy, new_entry, bs->dsk.clean_entry_size);
                    // The reads should free all metadata entry backups when they don't need them anymore
                    if (uo_it->first.version < new_entry->version)
                    {
                        // If ==, write in place
                        uo_it++;
                        bs->used_clean_objects.insert(uo_it, std::make_pair(ov, (used_clean_obj_t){
                            .meta = meta_copy,
                        }));
                    }
                    else
                    {
                        uo_it->second.meta = meta_copy;
                    }
                }
            }
        }
        // Set initial internal bitmap bits from the big write
        if (clean_init_bitmap)
        {
            memset(new_clean_bitmap, 0, bs->dsk.clean_entry_bitmap_size);
            bitmap_set(new_clean_bitmap, clean_bitmap_offset, clean_bitmap_len, bs->dsk.bitmap_granularity);
        }
        for (auto it = v.begin(); it != v.end(); it++)
        {
            // Set internal bitmap bits from small writes
            if (it->copy_flags == COPY_BUF_JOURNAL || it->copy_flags == (COPY_BUF_JOURNAL|COPY_BUF_COALESCED))
                bitmap_set(new_clean_bitmap, it->offset, it->len, bs->dsk.bitmap_granularity);
        }
        // Copy latest external bitmap/attributes
        {
            void *dyn_ptr = bs->alloc_dyn_data
                ? (uint8_t*)dirty_end->second.dyn_data+sizeof(int) : (uint8_t*)&dirty_end->second.dyn_data;
            memcpy(new_clean_bitmap + bs->dsk.clean_entry_bitmap_size, dyn_ptr, bs->dsk.clean_entry_bitmap_size);
        }
        // Copy initial (big_write) data checksums
        if (bs->dsk.csum_block_size && clean_init_bitmap)
        {
            uint8_t *new_clean_data_csum = new_clean_bitmap + 2*bs->dsk.clean_entry_bitmap_size;
            // big_write partial checksums are calculated from a padded csum_block_size, we can just copy them
            memset(new_clean_data_csum, 0, bs->dsk.data_block_size / bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
            uint64_t dyn_size = bs->dsk.dirty_dyn_size(clean_bitmap_offset, clean_bitmap_len);
            uint32_t *csums = (uint32_t*)(clean_init_dyn_ptr + bs->dsk.clean_entry_bitmap_size);
            memcpy(new_clean_data_csum + clean_bitmap_offset / bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF),
                csums, dyn_size - bs->dsk.clean_entry_bitmap_size);
        }
        // Calculate or copy small_write checksums
        uint32_t *new_data_csums = (uint32_t*)(new_clean_bitmap + 2*bs->dsk.clean_entry_bitmap_size);
        if (bs->dsk.csum_block_size)
            calc_block_checksums(new_data_csums, false);
        // Update entry
        new_entry->oid = cur.oid;
        new_entry->version = cur.version;
        if (!bs->inmemory_meta)
            memcpy(&new_entry->bitmap, new_clean_bitmap, bs->dsk.clean_dyn_size);
        if (bs->dsk.meta_format >= BLOCKSTORE_META_FORMAT_V2)
        {
            // Calculate metadata entry checksum
            uint32_t *new_entry_csum = (uint32_t*)((uint8_t*)new_entry + bs->dsk.clean_entry_size - 4);
            *new_entry_csum = crc32c(0, new_entry, bs->dsk.clean_entry_size - 4);
        }
    }
}

void journal_flusher_co::free_buffers()
{
    if (!bs->inmemory_meta)
    {
        meta_new.it->second.usage_count--;
        if (meta_new.it->second.usage_count == 0)
        {
            free(meta_new.it->second.buf);
            flusher->meta_sectors.erase(meta_new.it);
        }
        if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
        {
            meta_old.it->second.usage_count--;
            if (meta_old.it->second.usage_count == 0)
            {
                free(meta_old.it->second.buf);
                flusher->meta_sectors.erase(meta_old.it);
            }
        }
    }
    for (auto it = v.begin(); it != v.end(); it++)
    {
        // Free it if it's not taken from the journal
        if (it->buf && (it->copy_flags == COPY_BUF_JOURNAL || (it->copy_flags & COPY_BUF_CSUM_FILL)) &&
            (!bs->journal.inmemory || it->buf < bs->journal.buffer || it->buf >= (uint8_t*)bs->journal.buffer + bs->journal.len))
        {
            free(it->buf);
        }
    }
    v.clear();
}

bool journal_flusher_co::write_meta_block(flusher_meta_write_t & meta_block, int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    await_sqe(0);
    data->iov = (struct iovec){ meta_block.buf, bs->dsk.meta_block_size };
    data->callback = simple_callback_w;
    my_uring_prep_writev(
        sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bs->dsk.meta_block_size + meta_block.sector
    );
    wait_count++;
    return true;
}

bool journal_flusher_co::clear_incomplete_csum_block_bits(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    else if (wait_state == wait_base+3) goto resume_3;
    else if (wait_state == wait_base+4) goto resume_4;
    else if (wait_state == wait_base+5) goto resume_5;
    else if (wait_state == wait_base+6) goto resume_6;
    cleared_incomplete = false;
    for (auto it = v.begin(); it != v.end(); it++)
    {
        if ((it->copy_flags == COPY_BUF_JOURNAL || it->copy_flags == (COPY_BUF_JOURNAL|COPY_BUF_COALESCED)) &&
            bitmap_check(new_clean_bitmap, it->offset, it->len, bs->dsk.bitmap_granularity))
        {
            cleared_incomplete = true;
            break;
        }
    }
    if (cleared_incomplete)
    {
        // This modification may only happen in place
        assert(old_clean_loc == clean_loc);
        // Wait for data writes and metadata reads
    resume_0:
    resume_1:
        if (!wait_meta_reads(wait_base+0))
            return false;
        // Verify data checksums
        for (i = v.size()-1; i >= 0 && (v[i].copy_flags & COPY_BUF_CSUM_FILL); i--)
        {
            // If we encounter bad checksums during flush, we still update the bad block,
            // but intentionally mangle checksums to avoid hiding the corruption.
            iovec iov = { .iov_base = v[i].buf, .iov_len = v[i].len };
            if (!(v[i].copy_flags & COPY_BUF_JOURNAL))
            {
                assert(!(v[i].offset % bs->dsk.csum_block_size));
                assert(!(v[i].len % bs->dsk.csum_block_size));
                bs->verify_padded_checksums(new_clean_bitmap, false, v[i].offset, &iov, 1, [&](uint32_t bad_block, uint32_t calc_csum, uint32_t stored_csum)
                {
                    printf("Checksum mismatch in object %lx:%lx v%lu in data area at offset 0x%lx+0x%x: got %08x, expected %08x\n",
                        cur.oid.inode, cur.oid.stripe, old_clean_ver, old_clean_loc, bad_block, calc_csum, stored_csum);
                    for (uint32_t j = 0; j < bs->dsk.csum_block_size; j += bs->dsk.bitmap_granularity)
                    {
                        // Simplest method of mangling: flip one byte in every sector
                        ((uint8_t*)v[i].buf)[j+bad_block-v[i].offset] ^= 0xff;
                    }
                });
            }
            else
            {
                bs->verify_journal_checksums(v[i].csum_buf, v[i].offset, &iov, 1, [&](uint32_t bad_block, uint32_t calc_csum, uint32_t stored_csum)
                {
                    printf("Checksum mismatch in object %lx:%lx v%lu in journal at offset 0x%lx+0x%x (block offset 0x%lx): got %08x, expected %08x\n",
                        cur.oid.inode, cur.oid.stripe, old_clean_ver,
                        v[i].disk_offset, bad_block, v[i].offset, calc_csum, stored_csum);
                    bad_block += (v[i].offset/bs->dsk.csum_block_size) * bs->dsk.csum_block_size;
                    uint32_t bad_block_end = bad_block + bs->dsk.csum_block_size + (v[i].offset/bs->dsk.csum_block_size) * bs->dsk.csum_block_size;
                    if (bad_block < v[i].offset)
                        bad_block = v[i].offset;
                    if (bad_block_end > v[i].offset+v[i].len)
                        bad_block_end = v[i].offset+v[i].len;
                    bad_block -= v[i].offset;
                    bad_block_end -= v[i].offset;
                    for (uint32_t j = bad_block; j < bad_block_end; j += bs->dsk.bitmap_granularity)
                    {
                        // Simplest method of mangling: flip one byte in every sector
                        ((uint8_t*)v[i].buf)[j] ^= 0xff;
                    }
                });
            }
        }
        // Actually clear bits
        for (auto it = v.begin(); it != v.end(); it++)
        {
            if (it->copy_flags == COPY_BUF_JOURNAL || it->copy_flags == (COPY_BUF_JOURNAL|COPY_BUF_COALESCED))
                bitmap_clear(new_clean_bitmap, it->offset, it->len, bs->dsk.bitmap_granularity);
        }
        {
            clean_disk_entry *new_entry = (clean_disk_entry*)((uint8_t*)meta_new.buf + meta_new.pos*bs->dsk.clean_entry_size);
            if (new_entry->oid != cur.oid)
            {
                printf(
                    "Fatal error (metadata corruption or bug): tried to make holes in %lu (%lx:%lx v%lu) with %lx:%lx v%lu\n",
                    clean_loc >> bs->dsk.block_order, new_entry->oid.inode, new_entry->oid.stripe,
                    new_entry->version, cur.oid.inode, cur.oid.stripe, cur.version
                );
            }
            assert(new_entry->oid == cur.oid);
            // Calculate block checksums with new holes
            uint32_t *new_data_csums = (uint32_t*)(new_clean_bitmap + 2*bs->dsk.clean_entry_bitmap_size);
            calc_block_checksums(new_data_csums, true);
            if (!bs->inmemory_meta)
                memcpy(&new_entry->bitmap, new_clean_bitmap, bs->dsk.clean_dyn_size);
            if (bs->dsk.meta_format >= BLOCKSTORE_META_FORMAT_V2)
            {
                // calculate metadata entry checksum
                uint32_t *new_entry_csum = (uint32_t*)((uint8_t*)new_entry + bs->dsk.clean_entry_size - 4);
                *new_entry_csum = crc32c(0, new_entry, bs->dsk.clean_entry_size - 4);
            }
        }
        // Write and fsync the modified metadata entry
    resume_2:
        if (!write_meta_block(meta_new, wait_base+2))
            return false;
    resume_3:
        if (wait_count > 0)
        {
            wait_state = wait_base+3;
            return false;
        }
    resume_4:
    resume_5:
    resume_6:
        if (!fsync_batch(true, wait_base+4))
            return false;
    }
    return true;
}

void journal_flusher_co::calc_block_checksums(uint32_t *new_data_csums, bool skip_overwrites)
{
    uint64_t block_offset = 0;
    uint32_t block_done = 0;
    uint32_t block_csum = 0;
    for (auto it = v.begin(); it != v.end(); it++)
    {
        if (it->copy_flags & COPY_BUF_CSUM_FILL)
            break;
        if (block_done == 0)
        {
            // `v` should contain aligned items, possibly split into pieces
            assert(!(it->offset % bs->dsk.csum_block_size));
            block_offset = it->offset;
        }
        bool zero = (it->copy_flags & COPY_BUF_ZERO) || (skip_overwrites && (it->copy_flags & COPY_BUF_JOURNAL));
        auto len = it->len;
        while ((block_done+len) >= bs->dsk.csum_block_size)
        {
            if (!skip_overwrites && !block_done && it->csum_buf)
            {
                // We may take existing checksums if an overwrite contains a full block
                auto full_csum_offset = (it->offset+it->len-len+bs->dsk.csum_block_size-1) / bs->dsk.csum_block_size
                    - it->offset / bs->dsk.csum_block_size;
                auto full_csum_count = len/bs->dsk.csum_block_size;
                memcpy(new_data_csums + block_offset/bs->dsk.csum_block_size,
                    it->csum_buf + full_csum_offset*4, full_csum_count*4);
                len -= full_csum_count*bs->dsk.csum_block_size;
                block_offset += full_csum_count*bs->dsk.csum_block_size;
            }
            else
            {
                auto cur_len = bs->dsk.csum_block_size-block_done;
                block_csum = zero
                    ? crc32c_pad(block_csum, NULL, 0, cur_len, 0)
                    : crc32c(block_csum, (uint8_t*)it->buf+(it->len-len), cur_len);
                new_data_csums[block_offset / bs->dsk.csum_block_size] = block_csum;
                block_csum = 0;
                block_done = 0;
                block_offset += bs->dsk.csum_block_size;
                len -= cur_len;
            }
        }
        if (len > 0)
        {
            block_csum = zero
                ? crc32c_pad(block_csum, NULL, 0, len, 0)
                : crc32c(block_csum, (uint8_t*)it->buf+(it->len-len), len);
            block_done += len;
        }
    }
    // `v` should contain aligned items, possibly split into pieces
    assert(!block_done);
}

void journal_flusher_co::scan_dirty()
{
    dirty_it = dirty_start = dirty_end;
    v.clear();
    copy_count = 0;
    clean_loc = UINT64_MAX;
    has_delete = false;
    has_writes = false;
    skip_copy = false;
    clean_init_bitmap = false;
    fill_incomplete = false;
    read_to_fill_incomplete = 0;
    while (1)
    {
        if (!IS_STABLE(dirty_it->second.state))
        {
            char err[1024];
            snprintf(
                err, 1024, "BUG: Unexpected dirty_entry %lx:%lx v%lu unstable state during flush: 0x%x",
                dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version, dirty_it->second.state
            );
            throw std::runtime_error(err);
        }
        else if (IS_JOURNAL(dirty_it->second.state) && !skip_copy)
        {
            // Partial dirty overwrite
            has_writes = true;
            if (dirty_it->second.len != 0)
            {
                uint64_t offset = dirty_it->second.offset;
                uint64_t end_offset = dirty_it->second.offset + dirty_it->second.len;
                uint64_t blk_begin = 0, blk_end = 0;
                uint8_t *blk_buf = NULL;
                auto it = v.begin();
                while (end_offset > offset)
                {
                    for (; it != v.end() && !(it->copy_flags & COPY_BUF_CSUM_FILL); it++)
                        if (it->offset+it->len > offset)
                            break;
                    // If all items end before offset or if the found item starts after end_offset, just insert the buffer
                    // If (offset < it->offset < end_offset) insert (offset..it->offset) part
                    // If (it->offset <= offset <= it->offset+it->len) then just skip to it->offset+it->len
                    if (it == v.end() || (it->copy_flags & COPY_BUF_CSUM_FILL) || it->offset > offset)
                    {
                        uint64_t submit_len = it == v.end() || (it->copy_flags & COPY_BUF_CSUM_FILL) ||
                            it->offset >= end_offset ? end_offset-offset : it->offset-offset;
                        uint64_t submit_offset = dirty_it->second.location + offset - dirty_it->second.offset;
                        copy_count++;
                        it = v.insert(it, (copy_buffer_t){
                            .copy_flags = COPY_BUF_JOURNAL,
                            .offset = offset,
                            .len = submit_len,
                            .disk_offset = submit_offset,
                        });
                        if (bs->journal.inmemory)
                        {
                            // Take it from memory, don't copy it
                            it->buf = (uint8_t*)bs->journal.buffer + submit_offset;
                        }
                        if (bs->dsk.csum_block_size)
                        {
                            // FIXME Remove this > sizeof(void*) inline perversion from everywhere.
                            // I think it doesn't matter but I couldn't stop myself from implementing it :)
                            uint8_t* dyn_from = (uint8_t*)(bs->alloc_dyn_data
                                ? (uint8_t*)dirty_it->second.dyn_data+sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data) +
                                bs->dsk.clean_entry_bitmap_size;
                            it->csum_buf = dyn_from + (it->offset/bs->dsk.csum_block_size -
                                dirty_it->second.offset/bs->dsk.csum_block_size) * (bs->dsk.data_csum_type & 0xFF);
                            if (offset % bs->dsk.csum_block_size || submit_len % bs->dsk.csum_block_size)
                            {
                                // Small write not aligned for checksums. We may have to pad it
                                fill_incomplete = true;
                                if (!bs->journal.inmemory)
                                {
                                    bs->pad_journal_read(v, *it, dirty_it->second.offset,
                                        dirty_it->second.offset + dirty_it->second.len, dirty_it->second.location,
                                        dyn_from, NULL, offset, submit_len, blk_begin, blk_end, blk_buf);
                                }
                            }
                        }
                    }
                    offset = it->offset+it->len;
                }
            }
        }
        else if (IS_BIG_WRITE(dirty_it->second.state) && !skip_copy)
        {
            // There is an unflushed big write. Copy small writes in its position
            has_writes = true;
            clean_loc = dirty_it->second.location;
            clean_init_bitmap = true;
            clean_bitmap_offset = dirty_it->second.offset;
            clean_bitmap_len = dirty_it->second.len;
            clean_init_dyn_ptr = bs->alloc_dyn_data
                ? (uint8_t*)dirty_it->second.dyn_data+sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data;
            skip_copy = true;
        }
        else if (IS_DELETE(dirty_it->second.state) && !skip_copy)
        {
            // There is an unflushed delete
            has_delete = true;
            skip_copy = true;
        }
        dirty_start = dirty_it;
        if (dirty_it == bs->dirty_db.begin())
        {
            break;
        }
        dirty_it--;
        if (dirty_it->first.oid != cur.oid)
        {
            break;
        }
    }
    if (fill_incomplete && !clean_init_bitmap)
    {
        // Rescan and fill incomplete writes with old data to calculate checksums
        if (old_clean_loc == UINT64_MAX)
        {
            // May happen if the metadata entry is corrupt, but journal isn't
            // FIXME: Report corrupted object to the upper layer (OSD)
            printf(
                "Warning: object %lx:%lx has overwrites, but doesn't have a clean version."
                " Metadata is likely corrupted. Dropping object from the DB.\n",
                cur.oid.inode, cur.oid.stripe
            );
            v.clear();
            has_writes = false;
            has_delete = skip_copy = true;
            copy_count = 0;
            fill_incomplete = false;
            read_to_fill_incomplete = 0;
            return;
        }
        uint8_t *bmp_ptr = bs->get_clean_entry_bitmap(old_clean_loc, 0);
        uint64_t fulfilled = 0;
        read_to_fill_incomplete = bs->fill_partial_checksum_blocks(
            v, fulfilled, bmp_ptr, NULL, false, NULL, v[0].offset/bs->dsk.csum_block_size * bs->dsk.csum_block_size,
            ((v[v.size()-1].offset+v[v.size()-1].len-1) / bs->dsk.csum_block_size + 1) * bs->dsk.csum_block_size
        );
    }
    else if (fill_incomplete && clean_init_bitmap)
    {
        // If we actually have partial checksum block overwrites AND a new clean_loc
        // at the same time then we can't use our fancy checksum block mutation algorithm.
        // So in this case we'll have to first flush the clean write separately.
        while (!IS_BIG_WRITE(dirty_end->second.state))
        {
            assert(dirty_end != bs->dirty_db.begin());
            dirty_end--;
        }
        flusher->enqueue_flush(cur);
        cur.version = dirty_end->first.version;
#ifdef BLOCKSTORE_DEBUG
        printf("Partial checksum block overwrites found - rewinding flush back to %lx:%lx v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
        v.clear();
        copy_count = 0;
        fill_incomplete = false;
        read_to_fill_incomplete = 0;
    }
}

bool journal_flusher_co::read_dirty(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    wait_count = wait_journal_count = 0;
    if (bs->journal.inmemory && !read_to_fill_incomplete)
    {
        // Happy path: nothing to read :)
        return true;
    }
    for (i = 1; i <= v.size() && (v[v.size()-i].copy_flags & COPY_BUF_CSUM_FILL); i++)
    {
        if (v[v.size()-i].copy_flags & COPY_BUF_JOURNAL)
            continue;
        // Read old data from disk to calculate checksums
        await_sqe(0);
        auto & vi = v[v.size()-i];
        assert(vi.len != 0);
        vi.buf = memalign_or_die(MEM_ALIGNMENT, vi.len);
        data->iov = (struct iovec){ vi.buf, vi.len };
        data->callback = simple_callback_r;
        my_uring_prep_readv(
            sqe, bs->dsk.data_fd, &data->iov, 1, bs->dsk.data_offset + old_clean_loc + vi.offset
        );
        wait_count++;
        bs->find_holes(v, vi.offset, vi.offset+vi.len, [this, buf = (uint8_t*)vi.buf-vi.offset](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
        {
            if (!alloc)
            {
                v.insert(v.begin()+pos, (copy_buffer_t){
                    .copy_flags = COPY_BUF_DATA,
                    .offset = cur_start,
                    .len = cur_end-cur_start,
                    .buf = buf+cur_start,
                });
                return 1;
            }
            return 0;
        });
    }
    if (!bs->journal.inmemory)
    {
        for (i = 0; i < v.size(); i++)
        {
            if (v[i].copy_flags == COPY_BUF_JOURNAL ||
                v[i].copy_flags == (COPY_BUF_JOURNAL | COPY_BUF_CSUM_FILL))
            {
                // Read journal data from disk
                if (!v[i].buf)
                    v[i].buf = memalign_or_die(MEM_ALIGNMENT, v[i].len);
                await_sqe(1);
                data->iov = (struct iovec){ v[i].buf, (size_t)v[i].len };
                data->callback = simple_callback_rj;
                my_uring_prep_readv(
                    sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset + v[i].disk_offset
                );
                wait_journal_count++;
            }
        }
    }
    return true;
}

bool journal_flusher_co::modify_meta_do_reads(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
resume_0:
    if (!modify_meta_read(clean_loc, meta_new, wait_base+0))
        return false;
    new_clean_bitmap = (bs->inmemory_meta
        ? (uint8_t*)meta_new.buf + meta_new.pos*bs->dsk.clean_entry_size + sizeof(clean_disk_entry)
        : (uint8_t*)bs->clean_dyn_data + (clean_loc >> bs->dsk.block_order)*bs->dsk.clean_dyn_size);
    if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
    {
    resume_1:
        if (!modify_meta_read(old_clean_loc, meta_old, wait_base+1))
            return false;
    }
    else
        meta_old.submitted = false;
    return true;
}

bool journal_flusher_co::wait_meta_reads(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
resume_0:
    if (wait_count > 0)
    {
        wait_state = wait_base+0;
        return false;
    }
    // Our own reads completed
    if (meta_new.submitted)
    {
        meta_new.it->second.state = META_BLOCK_READ;
        bs->ringloop->wakeup();
    }
    if (meta_old.submitted)
    {
        meta_old.it->second.state = META_BLOCK_READ;
        bs->ringloop->wakeup();
    }
resume_1:
    if (!bs->inmemory_meta && (meta_new.it->second.state == META_BLOCK_UNREAD ||
        (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc) && meta_old.it->second.state == META_BLOCK_UNREAD))
    {
        // Metadata block is being read by another coroutine
        wait_state = wait_base+1;
        return false;
    }
    // All reads completed
    return true;
}

bool journal_flusher_co::modify_meta_read(uint64_t meta_loc, flusher_meta_write_t &wr, int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    // We must check if the same sector is already in memory if we don't keep all metadata in memory all the time.
    // And yet another option is to use LSM trees for metadata, but it sophisticates everything a lot,
    // so I'll avoid it as long as I can.
    wr.submitted = false;
    wr.sector = ((meta_loc >> bs->dsk.block_order) / (bs->dsk.meta_block_size / bs->dsk.clean_entry_size)) * bs->dsk.meta_block_size;
    wr.pos = ((meta_loc >> bs->dsk.block_order) % (bs->dsk.meta_block_size / bs->dsk.clean_entry_size));
    if (bs->inmemory_meta)
    {
        wr.buf = (uint8_t*)bs->metadata_buffer + wr.sector;
        return true;
    }
    wr.it = flusher->meta_sectors.find(wr.sector);
    if (wr.it == flusher->meta_sectors.end())
    {
        // Not in memory yet, read it
        wr.buf = memalign_or_die(MEM_ALIGNMENT, bs->dsk.meta_block_size);
        wr.it = flusher->meta_sectors.emplace(wr.sector, (meta_sector_t){
            .offset = wr.sector,
            .len = bs->dsk.meta_block_size,
            .state = META_BLOCK_UNREAD, // 0 = not read yet
            .buf = wr.buf,
            .usage_count = 1,
        }).first;
        await_sqe(0);
        data->iov = (struct iovec){ wr.it->second.buf, bs->dsk.meta_block_size };
        data->callback = simple_callback_r;
        wr.submitted = true;
        my_uring_prep_readv(
            sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bs->dsk.meta_block_size + wr.sector
        );
        wait_count++;
    }
    else
    {
        wr.buf = wr.it->second.buf;
        wr.it->second.usage_count++;
    }
    return true;
}

void journal_flusher_co::update_clean_db()
{
    auto & clean_db = bs->clean_db_shard(cur.oid);
    if (has_delete)
    {
        clean_db.erase(cur.oid);
    }
    else
    {
        clean_db[cur.oid] = {
            .version = cur.version,
            .location = clean_loc,
        };
    }
}

void journal_flusher_co::free_data_blocks()
{
    if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
    {
        auto uo_it = bs->used_clean_objects.find((obj_ver_id){ .oid = cur.oid, .version = old_clean_ver });
        bool used = uo_it != bs->used_clean_objects.end();
#ifdef BLOCKSTORE_DEBUG
        printf("%s block %lu from %lx:%lx v%lu (new location is %lu)\n",
            used ? "Postpone free" : "Free",
            old_clean_loc >> bs->dsk.block_order,
            cur.oid.inode, cur.oid.stripe, cur.version,
            clean_loc >> bs->dsk.block_order);
#endif
        if (used)
            uo_it->second.freed_block = 1 + (old_clean_loc >> bs->dsk.block_order);
        else
            bs->data_alloc->set(old_clean_loc >> bs->dsk.block_order, false);
    }
    if (has_delete)
    {
        assert(clean_loc == old_clean_loc);
        auto uo_it = bs->used_clean_objects.find((obj_ver_id){ .oid = cur.oid, .version = old_clean_ver });
        bool used = uo_it != bs->used_clean_objects.end();
#ifdef BLOCKSTORE_DEBUG
        printf("%s block %lu from %lx:%lx v%lu (delete)\n",
            used ? "Postpone free" : "Free",
            clean_loc >> bs->dsk.block_order,
            cur.oid.inode, cur.oid.stripe, cur.version);
#endif
        if (used)
            uo_it->second.freed_block = 1 + (clean_loc >> bs->dsk.block_order);
        else
            bs->data_alloc->set(clean_loc >> bs->dsk.block_order, false);
    }
}

bool journal_flusher_co::fsync_batch(bool fsync_meta, int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    if (!(fsync_meta ? bs->disable_meta_fsync : bs->disable_data_fsync))
    {
        cur_sync = flusher->syncs.end();
        while (cur_sync != flusher->syncs.begin())
        {
            cur_sync--;
            if (cur_sync->fsync_meta == fsync_meta && cur_sync->state == 0)
            {
                goto sync_found;
            }
        }
        cur_sync = flusher->syncs.emplace(flusher->syncs.end(), (flusher_sync_t){
            .fsync_meta = fsync_meta,
            .ready_count = 0,
            .state = 0,
        });
    sync_found:
        cur_sync->ready_count++;
        flusher->syncing_flushers++;
    resume_1:
        if (!cur_sync->state)
        {
            if (flusher->syncing_flushers >= flusher->active_flushers || !flusher->flush_queue.size())
            {
                // Sync batch is ready. Do it.
                await_sqe(0);
                data->iov = { 0 };
                data->callback = simple_callback_w;
                my_uring_prep_fsync(sqe, fsync_meta ? bs->dsk.meta_fd : bs->dsk.data_fd, IORING_FSYNC_DATASYNC);
                cur_sync->state = 1;
                wait_count++;
            resume_2:
                if (wait_count > 0)
                {
                    wait_state = wait_base+2;
                    return false;
                }
                // Sync completed. All previous coroutines waiting for it must be resumed
                cur_sync->state = 2;
                bs->ringloop->wakeup();
            }
            else
            {
                // Wait until someone else sends and completes a sync.
                wait_state = wait_base+1;
                return false;
            }
        }
        flusher->syncing_flushers--;
        cur_sync->ready_count--;
        if (cur_sync->ready_count == 0)
        {
            flusher->syncs.erase(cur_sync);
        }
    }
    return true;
}

bool journal_flusher_co::trim_journal(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    else if (wait_state == wait_base+3) goto resume_3;
    else if (wait_state == wait_base+4) goto resume_4;
    flusher->journal_trim_counter = 0;
    new_trim_pos = bs->journal.get_trim_pos();
    if (new_trim_pos != bs->journal.used_start)
    {
    resume_0:
        // Wait for other coroutines trimming the journal, if any
        if (flusher->trimming)
        {
            wait_state = wait_base+0;
            return false;
        }
        flusher->trimming = true;
        // Recheck the position with the "lock" taken
        new_trim_pos = bs->journal.get_trim_pos();
        if (new_trim_pos != bs->journal.used_start)
        {
            // First update journal "superblock" and only then update <used_start> in memory
            await_sqe(1);
            *((journal_entry_start*)flusher->journal_superblock) = {
                .crc32 = 0,
                .magic = JOURNAL_MAGIC,
                .type = JE_START,
                .size = ((!bs->dsk.data_csum_type && ((journal_entry_start*)flusher->journal_superblock)->version == JOURNAL_VERSION_V1)
                    ? (uint32_t)JE_START_V1_SIZE : (uint32_t)JE_START_V2_SIZE),
                .reserved = 0,
                .journal_start = new_trim_pos,
                .version = JOURNAL_VERSION_V2,
                .data_csum_type = bs->dsk.data_csum_type,
                .csum_block_size = bs->dsk.csum_block_size,
            };
            ((journal_entry_start*)flusher->journal_superblock)->crc32 = je_crc32((journal_entry*)flusher->journal_superblock);
            data->iov = (struct iovec){ flusher->journal_superblock, bs->dsk.journal_block_size };
            data->callback = simple_callback_w;
            my_uring_prep_writev(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset);
            wait_count++;
        resume_2:
            if (wait_count > 0)
            {
                wait_state = wait_base+2;
                return false;
            }
            if (!bs->disable_journal_fsync)
            {
                await_sqe(3);
                my_uring_prep_fsync(sqe, bs->dsk.journal_fd, IORING_FSYNC_DATASYNC);
                data->iov = { 0 };
                data->callback = simple_callback_w;
                wait_count++;
            resume_4:
                if (wait_count > 0)
                {
                    wait_state = wait_base+4;
                    return false;
                }
            }
            if (new_trim_pos < bs->journal.used_start
                ? (bs->journal.dirty_start >= bs->journal.used_start || bs->journal.dirty_start < new_trim_pos)
                : (bs->journal.dirty_start >= bs->journal.used_start && bs->journal.dirty_start < new_trim_pos))
            {
                bs->journal.dirty_start = new_trim_pos;
            }
            bs->journal.used_start = new_trim_pos;
#ifdef BLOCKSTORE_DEBUG
            printf("Journal trimmed to %08lx (next_free=%08lx dirty_start=%08lx)\n", bs->journal.used_start, bs->journal.next_free, bs->journal.dirty_start);
#endif
            if (bs->journal.flush_journal && !flusher->flush_queue.size())
            {
                assert(bs->journal.used_start == bs->journal.next_free);
                printf("Journal flushed\n");
                exit(0);
            }
        }
        flusher->trimming = false;
    }
    return true;
}
