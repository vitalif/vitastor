// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#define COPY_BUF_JOURNAL 1
#define COPY_BUF_DATA 2
#define COPY_BUF_ZERO 4
#define COPY_BUF_CSUM_FILL 8
#define COPY_BUF_COALESCED 16
#define COPY_BUF_META_BLOCK 32
#define COPY_BUF_JOURNALED_BIG 64

struct copy_buffer_t
{
    int copy_flags;
    uint64_t offset, len, disk_offset;
    uint64_t journal_sector; // only for reads: sector+1 if used and !journal.inmemory, otherwise 0
    void *buf;
    uint8_t *csum_buf;
    int *dyn_data;
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
    int wait_state, wait_count, wait_journal_count;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;

    std::list<flusher_sync_t>::iterator cur_sync;

    obj_ver_id cur;
    std::map<obj_ver_id, dirty_entry>::iterator dirty_it, dirty_start, dirty_end;
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_rj, simple_callback_w;

    bool try_trim = false;
    bool skip_copy, has_delete, has_writes;
    std::vector<copy_buffer_t> v;
    std::vector<copy_buffer_t>::iterator it;
    int i;
    bool fill_incomplete, cleared_incomplete;
    int read_to_fill_incomplete;
    int copy_count;
    uint64_t clean_loc, clean_ver, old_clean_loc, old_clean_ver;
    flusher_meta_write_t meta_old, meta_new;
    bool clean_init_bitmap;
    uint64_t clean_bitmap_offset, clean_bitmap_len;
    uint8_t *clean_init_dyn_ptr;
    uint8_t *new_clean_bitmap;

    uint64_t new_trim_pos;
    std::unordered_set<uint64_t>::iterator inflight_meta_sector;

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
    bool dequeuing;
    int min_flusher_count, max_flusher_count, cur_flusher_count, target_flusher_count;
    int flusher_start_threshold;
    journal_flusher_co *co;
    blockstore_impl_t *bs;
    friend class journal_flusher_co;

    int journal_trim_counter;
    bool trimming;
    void* journal_superblock;

    int active_flushers;
    int syncing_flushers;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

    std::map<uint64_t, meta_sector_t> meta_sectors;
    std::deque<object_id> flush_queue;
    std::map<object_id, uint64_t> flush_versions; // FIXME: consider unordered_map?
    std::unordered_set<uint64_t> inflight_meta_sectors;

    bool try_find_older(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur);
    bool try_find_other(std::map<obj_ver_id, dirty_entry>::iterator & dirty_end, obj_ver_id & cur);

public:
    journal_flusher_t(blockstore_impl_t *bs);
    ~journal_flusher_t();
    void loop();
    bool is_trim_wanted() { return trim_wanted; }
    bool is_active();
    void mark_trim_possible();
    void request_trim();
    void release_trim();
    void enqueue_flush(obj_ver_id oid);
    void unshift_flush(obj_ver_id oid, bool force);
    void remove_flush(object_id oid);
    void dump_diagnostics();
    bool is_mutated(uint64_t clean_loc);
};
