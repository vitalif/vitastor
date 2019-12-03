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
    blockstore *bs;
    journal_flusher_t *flusher;
    int wait_state, wait_count;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    bool skip_copy, has_delete;
    obj_ver_id cur;
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it, dirty_start, dirty_end;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    int copy_count;
    uint64_t offset, end_offset, submit_offset, submit_len, clean_loc, old_clean_loc, old_clean_ver;
    flusher_meta_write_t meta_old, meta_new;
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_w;
    std::list<flusher_sync_t>::iterator cur_sync;
    friend class journal_flusher_t;
    bool modify_meta_read(uint64_t meta_loc, flusher_meta_write_t &wr, int wait_base);
    void update_clean_db();
    bool fsync_batch(bool fsync_meta, int wait_base);
public:
    journal_flusher_co();
    bool loop();
};

// Journal flusher itself
class journal_flusher_t
{
    bool start_forced = false;
    int flusher_count;
    int sync_threshold;
    journal_flusher_co *co;
    blockstore *bs;
    friend class journal_flusher_co;

    int journal_trim_counter, journal_trim_interval;
    void* journal_superblock;

    int active_flushers;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<object_id> flush_queue;
    std::map<object_id, uint64_t> flush_versions;
public:
    journal_flusher_t(int flusher_count, blockstore *bs);
    ~journal_flusher_t();
    void loop();
    bool is_active();
    void force_start();
    void enqueue_flush(obj_ver_id oid);
    void unshift_flush(obj_ver_id oid);
};
