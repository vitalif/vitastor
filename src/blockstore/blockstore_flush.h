// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

struct copy_buffer_t
{
    uint32_t copy_flags;
    uint64_t offset, len, disk_loc, disk_offset, disk_len;
    uint8_t *buf;
    heap_entry_t *wr;
};

struct meta_sector_t
{
    uint64_t offset, len;
    int state;
    void *buf;
    int usage_count;
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
    int co_id;
    int wait_state, wait_count;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    uint8_t *new_csums = NULL;
    uint8_t *new_bmp = NULL;
    uint8_t *punch_bmp = NULL;
    uint8_t *new_ext_bmp = NULL;

    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_w;

    object_id cur_oid;
    heap_entry_t *cur_obj;
    uint64_t fsynced_lsn;
    heap_compact_t compact_info;
    uint64_t clean_loc;
    uint32_t modified_block;
    bool bitmap_copied;
    bool should_repeat;

    std::vector<copy_buffer_t> read_vec;
    std::vector<heap_entry_t*> csum_copy;
    uint32_t overwrite_start, overwrite_end;
    int i, res;
    bool read_to_fill_incomplete;
    int copy_count;
    bool do_repeat = false;

    friend class journal_flusher_t;

    void iterate_checksum_holes(std::function<void(int & pos, uint32_t hole_start, uint32_t hole_end)> cb);
    void fill_partial_checksum_blocks();
    void free_buffers();
    int check_and_punch_checksums();
    bool calc_block_checksums();
    bool write_meta_block(int wait_base);
    bool read_buffered(int wait_base);
    bool fsync_meta(int wait_base);
    bool fsync_buffer(int wait_base);
    bool trim_lsn(int wait_base);
public:
    journal_flusher_co();
    ~journal_flusher_co();
    bool loop();
};

// Journal flusher itself
class journal_flusher_t
{
    int force_start = 0;
    int min_flusher_count = 0, max_flusher_count = 0, cur_flusher_count = 0, target_flusher_count = 0;
    journal_flusher_co *co;
    blockstore_impl_t *bs;
    friend class journal_flusher_co;

    uint64_t compact_counter = 0;

    robin_hood::unordered_flat_set<object_id> flushing;
    int active_flushers = 0;
    int wanting_meta_fsync = 0;
    bool fsyncing_meta = false;
    int syncing_buffer = 0;

public:
    journal_flusher_t(blockstore_impl_t *bs);
    ~journal_flusher_t();
    void loop();
    int get_syncing_buffer();
    uint64_t get_compact_counter();
    bool is_active();
    void request_trim();
    void release_trim();
    void dump_diagnostics();
};
