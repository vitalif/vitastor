// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

struct copy_buffer_t
{
    int copy_flags;
    uint64_t offset, len, disk_offset, disk_len;
    uint8_t *buf;
    uint32_t wr_offset;
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
    std::map<object_id, uint64_t>::iterator repeat_it;
    std::function<void(ring_data_t*)> simple_callback_r, simple_callback_w;

    object_id cur_oid;
    uint64_t cur_lsn;
    uint64_t compact_lsn;
    uint64_t min_compact_lsn;
    uint64_t cur_version;
    heap_object_t *cur_obj;
    heap_write_t *begin_wr, *end_wr;
    uint32_t modified_block;

    std::vector<copy_buffer_t> read_vec;
    uint32_t overwrite_start, overwrite_end;
    int i, res;
    bool read_to_fill_incomplete;
    int copy_count;
    uint64_t clean_loc;
    flusher_meta_write_t meta_old, meta_new;
    uint8_t *csum_buf = NULL;
    uint8_t *new_data_csums = NULL;
    bool do_repeat = false;

    friend class journal_flusher_t;

    void iterate_partial_overwrites(std::function<int(int, uint32_t, uint32_t)> cb);
    void iterate_checksum_holes(std::function<void(int, uint32_t, uint32_t)> cb);
    void fill_partial_checksum_blocks();
    void free_buffers();
    int check_and_punch_checksums();
    void calc_block_checksums();
    bool write_meta_block(int wait_base);
    bool read_buffered(int wait_base);
    bool fsync_batch(bool fsync_meta, int wait_base);
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

    int advance_lsn_counter = 0;

    int active_flushers = 0;
    int syncing_flushers = 0;
    std::list<flusher_sync_t> syncs;
    std::map<object_id, uint64_t> sync_to_repeat;

public:
    journal_flusher_t(blockstore_impl_t *bs);
    ~journal_flusher_t();
    void loop();
    bool is_active();
    void request_trim();
    void release_trim();
    void dump_diagnostics();
};
