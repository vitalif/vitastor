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
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    uint64_t offset, len, submit_len, clean_loc, meta_sector, meta_pos;
    std::map<uint64_t, meta_sector_t>::iterator meta_it;
    friend class journal_flusher_t;
public:
    void loop();
};

// Journal flusher itself
class journal_flusher_t
{
    int flusher_count;
    int active_flushers;
    journal_flusher_co *co;
    blockstore *bs;
    friend class journal_flusher_co;
public:
    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<obj_ver_id> flush_queue;
    journal_flusher_t(int flusher_count, blockstore *bs);
    ~journal_flusher_t();
    void loop();
};
