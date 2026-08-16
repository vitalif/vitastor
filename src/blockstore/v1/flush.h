// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

struct copy_buffer_t
{
    int copy_flags = 0;
    uint64_t offset = 0, len = 0, disk_offset = 0;
    uint64_t journal_sector = 0; // only for reads: sector+1 if used and !journal.inmemory, otherwise 0
    void *buf = NULL;
    uint8_t *csum_buf = NULL;
    int *dyn_data = NULL;
};

struct meta_sector_t
{
    uint64_t offset = 0, len = 0;
    int state = 0;
    void *buf = NULL;
    int usage_count = 0;
};

struct flusher_sync_t
{
    bool fsync_meta = false;
    int ready_count = 0;
    int state = 0;
};

struct flusher_meta_write_t
{
    uint64_t sector = 0, pos = 0;
    bool submitted = 0;
    void *buf = NULL;
    std::map<uint64_t, meta_sector_t>::iterator it;
};

class journal_flusher_t;

// Journal flusher coroutine
class journal_flusher_co
{
    blockstore_impl_t *bs = NULL;
    journal_flusher_t *flusher = NULL;
    int wait_state = 0, wait_count = 0, wait_journal_count = 0;
    struct io_uring_sqe *sqe = NULL;
    struct ring_data_t *data = NULL;

    std::list<flusher_sync_t>::iterator cur_sync;

    obj_ver_id cur = {};
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it, dirty_start, dirty_end;
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_rj, simple_callback_w;

    bool try_trim = false;
    bool skip_copy = false, has_delete = false, has_writes = false;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    int i = 0;
    bool fill_incomplete = false, cleared_incomplete = false;
    int read_to_fill_incomplete = 0;
    int copy_count = 0;
    uint64_t clean_loc = 0, clean_ver = 0, old_clean_loc = 0, old_clean_ver = 0;
    flusher_meta_write_t meta_old, meta_new;
    bool clean_init_bitmap = false;
    uint64_t clean_bitmap_offset = 0, clean_bitmap_len = 0;
    uint8_t *clean_init_dyn_ptr = NULL;
    uint8_t *new_clean_bitmap = NULL;
    std::unordered_set<uint32_t> mangle_csum_blocks;

    uint64_t new_trim_pos = 0;

    friend class journal_flusher_t;
    void scan_dirty();
    bool read_dirty(int wait_base);
    bool modify_meta_do_reads(int wait_base);
    bool wait_meta_reads(int wait_base);
    bool modify_meta_read(uint64_t meta_loc, flusher_meta_write_t &wr, int wait_base);
    bool clear_incomplete_csum_block_bits(int wait_base);
    void calc_block_checksums(uint32_t *new_data_csums, bool skip_overwrites);
    void update_metadata_entry();
    bool write_meta_block(flusher_meta_write_t & meta_block, int wait_base);
    void update_clean_db();
    void free_data_blocks();
    bool fsync_batch(bool fsync_meta, int wait_base);
    bool trim_journal(int wait_base);
    void free_buffers();
public:
    journal_flusher_co();
    bool loop();
};

// Journal flusher itself
class journal_flusher_t
{
    int trim_wanted = 0;
    bool trim_possible = false;
    bool dequeuing = false;
    int min_flusher_count = 0, max_flusher_count = 0, cur_flusher_count = 0, target_flusher_count = 0;
    int flusher_start_threshold = 0;
    journal_flusher_co *co = NULL;
    blockstore_impl_t *bs = NULL;
    friend class journal_flusher_co;

    int journal_trim_counter = 0;
    bool trimming = false;
    void* journal_superblock = NULL;

    int active_flushers = 0;
    int syncing_flushers = 0;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<object_id> flush_queue;
    std::unordered_map<object_id, uint64_t> flush_versions;
    std::unordered_set<uint64_t> inflight_meta_sectors;

    bool try_find_older(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur);
    bool try_find_other(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur);
    bool may_advance_except_one();

public:
    journal_flusher_t(blockstore_impl_t *bs);
    ~journal_flusher_t();
    void loop();
    bool is_trim_wanted() { return trim_wanted; }
    bool may_advance();
    bool is_active();
    size_t get_queue_size();
    void mark_trim_possible();
    void request_trim();
    void release_trim();
    void enqueue_flush(obj_ver_id oid);
    void unshift_flush(obj_ver_id oid, bool force);
    void remove_flush(object_id oid);
    void dump_diagnostics();
    bool is_mutated(uint64_t clean_loc);
};
