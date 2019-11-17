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
    int ready_count;
    int state;
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
    bool skip_copy;
    obj_ver_id cur;
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it, dirty_start, dirty_end;
    spp::sparse_hash_map<object_id, clean_entry, oid_hash>::iterator clean_it;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    uint64_t offset, len, submit_len, clean_loc, meta_sector, meta_pos;
    std::map<uint64_t, meta_sector_t>::iterator meta_it;
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::map<uint64_t, uint64_t>::iterator journal_used_it;
    std::function<void(ring_data_t*)> simple_callback;
    std::list<flusher_sync_t>::iterator cur_sync;
    friend class journal_flusher_t;
public:
    journal_flusher_co();
    void loop();
};

// Journal flusher itself
class journal_flusher_t
{
    int flusher_count;
    int sync_threshold;
    bool sync_required;
    journal_flusher_co *co;
    blockstore *bs;
    friend class journal_flusher_co;

    int journal_trim_counter, journal_trim_interval;
    uint8_t* journal_superblock;

    int active_flushers, active_until_sync;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<object_id> flush_queue;
    std::map<object_id, uint64_t> flush_versions;
public:
    journal_flusher_t(int flusher_count, blockstore *bs);
    ~journal_flusher_t();
    void loop();
    void queue_flush(obj_ver_id oid);
    void unshift_flush(obj_ver_id oid);
};