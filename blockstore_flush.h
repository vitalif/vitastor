// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

struct copy_buffer_t
{
    uint64_t offset, len;
    void *buf;
};

struct meta_sector_t
{
    uint64_t offset, len;
    int state;
    void *buf;
    int usage_count;
};

struct flusher_sync_t
{
    bool fsync_meta;
    int ready_count;
    int state;
};

struct flusher_meta_write_t
{
    uint64_t sector, pos;
    bool submitted;
    void *buf;
    std::map<uint64_t, meta_sector_t>::iterator it;
};

class journal_flusher_t;

// Journal flusher coroutine
class journal_flusher_co
{
    blockstore_impl_t *bs;
    journal_flusher_t *flusher;
    int wait_state, wait_count;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;

    std::list<flusher_sync_t>::iterator cur_sync;

    obj_ver_id cur;
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it, dirty_start, dirty_end;
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_w;

    bool skip_copy, has_delete, has_writes;
    blockstore_clean_db_t::iterator clean_it;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    int copy_count;
    uint64_t clean_loc, old_clean_loc;
    flusher_meta_write_t meta_old, meta_new;
    bool clean_init_bitmap;
    uint64_t clean_bitmap_offset, clean_bitmap_len;
    void *new_clean_bitmap;

    uint64_t new_trim_pos;

    // local: scan_dirty()
    uint64_t offset, end_offset, submit_offset, submit_len;

    friend class journal_flusher_t;
    bool scan_dirty(int wait_base);
    bool modify_meta_read(uint64_t meta_loc, flusher_meta_write_t &wr, int wait_base);
    void update_clean_db();
    bool fsync_batch(bool fsync_meta, int wait_base);
    void bitmap_set(void *bitmap, uint64_t start, uint64_t len);
public:
    journal_flusher_co();
    bool loop();
};

// Journal flusher itself
class journal_flusher_t
{
    int trim_wanted = 0;
    bool dequeuing;
    int flusher_count;
    int flusher_start_threshold;
    journal_flusher_co *co;
    blockstore_impl_t *bs;
    friend class journal_flusher_co;

    int journal_trim_counter, journal_trim_interval;
    bool trimming;
    void* journal_superblock;

    int active_flushers;
    int syncing_flushers;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<object_id> flush_queue;
    std::map<object_id, uint64_t> flush_versions;
public:
    journal_flusher_t(int flusher_count, blockstore_impl_t *bs);
    ~journal_flusher_t();
    void loop();
    bool is_active();
    void mark_trim_possible();
    void request_trim();
    void release_trim();
    void enqueue_flush(obj_ver_id oid);
    void unshift_flush(obj_ver_id oid);
};
