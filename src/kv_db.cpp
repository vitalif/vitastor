// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Vitastor shared key/value database
// Parallel optimistic B-Tree :-)

#define _XOPEN_SOURCE
#include <limits.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
//#include <signal.h>

#include "cluster_client.h"
#include "str_util.h"
#include "kv_db.h"

// 0x VITASTOR OPTBTREE
#define KV_BLOCK_MAGIC 0x761A5106097B18EE
#define KV_BLOCK_MAX_ITEMS 1048576
#define KV_INDEX_MAX_SIZE (uint64_t)1024*1024*1024*1024

#define KV_GET_CACHED 1
#define KV_GET 2
#define KV_SET 3
#define KV_DEL 4
#define KV_LIST 5

#define KV_INT 1
#define KV_INT_SPLIT 2
#define KV_LEAF 3
#define KV_LEAF_SPLIT 4
#define KV_EMPTY 5

#define KV_RECHECK_NONE 0
#define KV_RECHECK_LEAF 1
#define KV_RECHECK_ALL 2

#define KV_CH_ADD 1
#define KV_CH_DEL 2
// UPD=ADD|DEL
#define KV_CH_UPD 3
#define KV_CH_SPLIT 4
#define KV_CH_CLEAR_RIGHT 8

#define LEVEL_BITS 8
#define NO_LEVEL_MASK (((uint64_t)1 << (64-LEVEL_BITS)) - 1)

struct __attribute__((__packed__)) kv_stored_block_t
{
    uint64_t magic;
    uint32_t block_size;
    uint32_t type; // KV_*
    uint64_t items; // number of items
    // root/int nodes: { delimiter_len, delimiter..., 8, <block_offset> }[]
    // leaf nodes: { key_len, key..., value_len, value... }[]
    uint8_t data[0];
};

struct kv_block_t
{
    // level of the block. root block has level equal to -db->base_block_level
    int level;
    // usage flag. set to db->usage_counter when block is used
    int usage;
    // current data size, to estimate whether the block can fit more items
    uint32_t data_size;
    uint32_t type;
    uint64_t offset;
    // block only contains keys in [key_ge, key_lt). I.e. key_ge <= key < key_lt.
    std::string key_ge, key_lt;
    // KV_INT_SPLIT/KV_LEAF_SPLIT nodes also contain one reference to another block
    // with keys in [right_half, key_lt)
    // This is required for CAS to guarantee consistency during split
    std::string right_half;
    uint64_t right_half_block;
    // non-leaf nodes: ( MIN_BOUND_i => BLOCK_i )[]
    // leaf nodes: ( KEY_i => VALUE_i )[]
    // FIXME: use flat map?
    std::map<std::string, std::string> data;

    // set during update
    int updating = 0;
    bool invalidated = false;
    int change_type;
    std::string change_key, change_value;
    std::string change_rh;
    uint64_t change_rh_block;

    void set_data_size();
    static int kv_size(const std::string & key, const std::string & value);
    int parse(uint64_t offset, uint8_t *data, int size);
    bool serialize(uint8_t *data, int size);
    void apply_change();
    void cancel_change();
    void dump(int base_level);
};

void kv_block_t::set_data_size()
{
    data_size = sizeof(kv_stored_block_t) + 4*2 + key_ge.size() + key_lt.size();
    if (this->type == KV_INT_SPLIT || this->type == KV_LEAF_SPLIT)
        data_size += 4 + right_half.size() + 8;
    for (auto & kv: data)
        data_size += kv_size(kv.first, kv.second);
}

int kv_block_t::kv_size(const std::string & key, const std::string & value)
{
    return 4*2 + key.size() + value.size();
}

struct kv_continue_write_t
{
    kv_block_t *blk;
    std::function<void(int)> cb;
};

struct kv_db_t
{
    cluster_client_t *cli = NULL;

    // config. maybe should be moved to a separate structure
    inode_t inode_id = 0;
    uint64_t next_free = 0;
    uint32_t kv_block_size = 0;
    uint32_t ino_block_size = 0;
    bool immediate_commit = false;
    uint64_t memory_limit = 128*1024*1024;
    uint64_t evict_unused_age = 1000;
    uint64_t evict_max_misses = 10;
    uint64_t evict_attempts_per_level = 3;
    uint64_t allocate_blocks = 4;
    uint64_t log_level = 1;

    // state
    uint64_t evict_unused_counter = 0;
    uint64_t cache_max_blocks = 0;
    int base_block_level = 0;
    int usage_counter = 1;
    int allocating_block_pos = 0;
    std::vector<uint64_t> allocating_blocks;
    std::set<uint64_t> block_levels;
    std::map<uint64_t, kv_block_t> block_cache;
    std::map<uint64_t, uint64_t> known_versions;
    std::map<uint64_t, uint64_t> new_versions;
    std::multimap<uint64_t, kv_continue_write_t> continue_write;
    std::multimap<uint64_t, std::function<void()>> continue_update;

    bool closing = false;
    int active_ops = 0;
    std::function<void()> on_close;

    uint64_t alloc_block();
    void clear_allocation_block(uint64_t offset);

    void open(inode_t inode_id, json11::Json cfg, std::function<void(int)> cb);
    void set_config(json11::Json cfg);
    void close(std::function<void()> cb);

    void find_size(uint64_t min, uint64_t max, int phase, std::function<void(int, uint64_t)> cb);
    void run_continue_update(uint64_t offset);
    void stop_updating(kv_block_t *blk);
};

struct kv_path_t
{
    uint64_t offset;
    uint64_t version;
};

struct kv_op_t
{
    kv_db_t *db;
    int opcode;
    std::string key, value;
    int res;
    bool done = false;
    std::function<void(kv_op_t *)> callback;
    std::function<bool(int res, const std::string & value)> cas_cb;

    void exec();
    void next(); // for list
protected:
    int recheck_policy = KV_RECHECK_LEAF;
    bool started = false;
    uint64_t cur_block = 0;
    std::string prev_key_ge, prev_key_lt;
    int cur_level = 0;
    std::vector<kv_path_t> path;
    bool updated = false;
    int retry = 0;
    bool skip_equal = false;

    void finish(int res);
    void get();
    int handle_block(int res, bool updated, bool stop_on_split);

    void update();
    void update_find();
    void create_root();
    void resume_split();
    void update_block(int path_pos, bool is_delete, const std::string & key, const std::string & value, std::function<void(int)> cb);

    void next_handle_block(int res, bool updated);
    void next_get();
    void next_go_up();
};

static std::string read_string(uint8_t *data, int size, int *pos)
{
    if (*pos+4 > size)
    {
        *pos = -1;
        return "";
    }
    uint32_t len = *(uint32_t*)(data+*pos);
    *pos += sizeof(uint32_t);
    if (*pos+len > size)
    {
        *pos = -1;
        return "";
    }
    std::string key((char*)data+*pos, len);
    *pos += len;
    return key;
}

int kv_block_t::parse(uint64_t offset, uint8_t *data, int size)
{
    kv_stored_block_t *blk = (kv_stored_block_t *)data;
    if (blk->magic == 0 || blk->type == KV_EMPTY)
    {
        // empty block
        if (offset != 0)
            fprintf(stderr, "K/V: Block %lu is %s\n", offset, blk->magic == 0 ? "empty" : "cleared");
        return -ENOTBLK;
    }
    if (blk->magic != KV_BLOCK_MAGIC || blk->block_size != size ||
        !blk->type || blk->type > KV_EMPTY || blk->items > KV_BLOCK_MAX_ITEMS)
    {
        // invalid block
        fprintf(stderr, "K/V: Invalid block %lu magic, size, type or item count\n", offset);
        return -EILSEQ;
    }
    this->type = blk->type;
    int pos = blk->data - data;
    this->key_ge = read_string(data, size, &pos);
    if (pos < 0)
    {
        fprintf(stderr, "K/V: Invalid block %lu left bound\n", offset);
        return -EILSEQ;
    }
    this->key_lt = read_string(data, size, &pos);
    if (pos < 0)
    {
        fprintf(stderr, "K/V: Invalid block %lu right bound\n", offset);
        return -EILSEQ;
    }
    if (this->type == KV_INT_SPLIT || this->type == KV_LEAF_SPLIT)
    {
        this->right_half = read_string(data, size, &pos);
        if (pos < 0)
        {
            fprintf(stderr, "K/V: Invalid block %lu split bound\n", offset);
            return -EILSEQ;
        }
        if (pos+8 > size)
        {
            fprintf(stderr, "K/V: Invalid block %lu split block ref\n", offset);
            return -EILSEQ;
        }
        this->right_half_block = *(uint64_t*)(data+pos);
        pos += 8;
    }
    for (int i = 0; i < blk->items; i++)
    {
        auto key = read_string(data, size, &pos);
        if (pos < 0)
        {
            fprintf(stderr, "K/V: Invalid block %lu key %d\n", offset, i);
            return -EILSEQ;
        }
        auto value = read_string(data, size, &pos);
        if (pos < 0)
        {
            fprintf(stderr, "K/V: Invalid block %lu value %d\n", offset, i);
            return -EILSEQ;
        }
        this->data[key] = value;
    }
    this->data_size = pos;
    this->offset = offset;
    return 0;
}

static bool write_string(uint8_t *data, int size, int *pos, const std::string & s)
{
    if (*pos+s.size()+4 > size)
        return false;
    *(uint32_t*)(data+*pos) = s.size();
    *pos += 4;
    memcpy(data+*pos, s.data(), s.size());
    *pos += s.size();
    return true;
}

bool kv_block_t::serialize(uint8_t *buf, int size)
{
    kv_stored_block_t *blk = (kv_stored_block_t *)buf;
    blk->magic = KV_BLOCK_MAGIC;
    blk->block_size = size;
    if ((change_type & KV_CH_CLEAR_RIGHT))
    {
        if (type == KV_LEAF_SPLIT)
            blk->type = KV_LEAF;
        else if (type == KV_INT_SPLIT)
            blk->type = KV_INT;
        else
            blk->type = type;
    }
    else if ((change_type & KV_CH_SPLIT))
    {
        if (type == KV_LEAF)
            blk->type = KV_LEAF_SPLIT;
        else if (type == KV_INT)
            blk->type = KV_INT_SPLIT;
        else
            blk->type = type;
    }
    else
        blk->type = type;
    int pos = blk->data - buf;
    if (!write_string(buf, size, &pos, key_ge))
        return false;
    if (!write_string(buf, size, &pos, (change_type & KV_CH_CLEAR_RIGHT) ? right_half : key_lt))
        return false;
    if (blk->type == KV_LEAF_SPLIT || blk->type == KV_INT_SPLIT)
    {
        if (!write_string(buf, size, &pos, (change_type & KV_CH_SPLIT) ? change_rh : right_half))
            return false;
        if (pos+8 > size)
            return false;
        *(uint64_t*)(buf+pos) = (change_type & KV_CH_SPLIT) ? change_rh_block : right_half_block;
        pos += 8;
    }
    auto old_it = (change_type & KV_CH_UPD) ? data.lower_bound(change_key) : data.end();
    auto end_it = (change_type & KV_CH_SPLIT) ? data.lower_bound(change_rh) : data.end();
    blk->items = 0;
    for (auto kv_it = data.begin(); kv_it != end_it; kv_it++)
    {
        if (!(change_type & KV_CH_DEL) || kv_it != old_it || old_it->first != change_key)
        {
            if (!write_string(buf, size, &pos, kv_it->first) ||
                !write_string(buf, size, &pos, kv_it->second))
                return false;
            blk->items++;
        }
        if ((change_type & KV_CH_ADD) && kv_it == old_it)
        {
            if (!write_string(buf, size, &pos, change_key) ||
                !write_string(buf, size, &pos, change_value))
                return false;
            blk->items++;
        }
    }
    if ((change_type & KV_CH_ADD) && end_it == old_it)
    {
        if (!write_string(buf, size, &pos, change_key) ||
            !write_string(buf, size, &pos, change_value))
            return false;
        blk->items++;
    }
    return true;
}

void kv_block_t::apply_change()
{
    if ((change_type & KV_CH_UPD) == KV_CH_DEL)
    {
        auto kv_it = data.find(change_key);
        assert(kv_it != data.end());
        data_size -= kv_block_t::kv_size(kv_it->first, kv_it->second);
        data.erase(kv_it);
    }
    if ((change_type & KV_CH_ADD))
    {
        auto kv_it = data.find(change_key);
        if (kv_it != data.end())
            data_size -= kv_block_t::kv_size(kv_it->first, kv_it->second);
        data_size += kv_block_t::kv_size(change_key, change_value);
        data[change_key] = change_value;
    }
    if ((change_type & KV_CH_CLEAR_RIGHT) && (type == KV_INT_SPLIT || type == KV_LEAF_SPLIT))
    {
        type = (type == KV_LEAF_SPLIT ? KV_LEAF : KV_INT);
        key_lt = right_half;
        right_half = "";
        right_half_block = 0;
        set_data_size();
    }
    else if ((change_type & KV_CH_SPLIT) && (type == KV_INT || type == KV_LEAF))
    {
        type = (type == KV_LEAF ? KV_LEAF_SPLIT : KV_INT_SPLIT);
        right_half = change_rh;
        right_half_block = change_rh_block;
        data.erase(data.lower_bound(change_rh), data.end());
        set_data_size();
    }
    change_type = 0;
    change_key = change_value = change_rh = "";
    change_rh_block = 0;
}

void kv_block_t::cancel_change()
{
    change_type = 0;
    apply_change();
}

static const char *block_type_names[] = {
    "unknown",
    "int",
    "int_split",
    "leaf",
    "leaf_split",
    "empty",
};

static void dump_str(const std::string & str)
{
    size_t pos = 0;
    fwrite("\"", 1, 1, stdout);
    while (true)
    {
        auto pos2 = str.find('"', pos);
        if (pos2 == std::string::npos)
        {
            fwrite(str.data()+pos, str.size()-pos, 1, stdout);
            break;
        }
        else
        {
            fwrite(str.data()+pos, pos2-pos, 1, stdout);
            fwrite("\\\"", 2, 1, stdout);
            pos = pos2+1;
        }
    }
    fwrite("\"", 1, 1, stdout);
}

void kv_block_t::dump(int base_level)
{
    printf(
        "{\n    \"block\": %lu,\n    \"level\": %d,\n    \"type\": \"%s\",\n    \"range\": [",
        offset, base_level+level,
        type < sizeof(block_type_names)/sizeof(block_type_names[0]) ? block_type_names[type] : "unknown"
    );
    dump_str(key_ge);
    printf(", ");
    dump_str(key_lt);
    printf("],\n");
    if (type == KV_INT_SPLIT || type == KV_LEAF_SPLIT)
    {
        printf("    \"right_half\": { ");
        dump_str(right_half);
        printf(": %lu },\n", right_half_block);
    }
    printf("    \"data\": {\n");
    for (auto & kv: data)
    {
        printf("        ");
        dump_str(kv.first);
        printf(": ");
        if (type == KV_LEAF || type == KV_LEAF_SPLIT || kv.second.size() != 8)
            dump_str(kv.second);
        else
            printf("%lu", *(uint64_t*)kv.second.c_str());
        printf(",\n");
    }
    printf("    }\n}\n");
}

void kv_db_t::open(inode_t inode_id, json11::Json cfg, std::function<void(int)> cb)
{
    if (block_cache.size() > 0)
    {
        cb(-EINVAL);
        return;
    }
    auto pool_it = cli->st_cli.pool_config.find(INODE_POOL(inode_id));
    if (pool_it == cli->st_cli.pool_config.end())
    {
        cb(-EINVAL);
        return;
    }
    auto & pool_cfg = pool_it->second;
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t kv_block_size = cfg["kv_block_size"].uint64_value();
    if (!kv_block_size)
        kv_block_size = 4096;
    if ((pool_cfg.data_block_size*pg_data_size) % kv_block_size ||
        kv_block_size < pool_cfg.bitmap_granularity)
    {
        cb(-EINVAL);
        return;
    }
    this->inode_id = inode_id;
    this->immediate_commit = cli->get_immediate_commit(inode_id);
    this->ino_block_size = pool_cfg.data_block_size * pg_data_size;
    this->kv_block_size = kv_block_size;
    this->next_free = 0;
    set_config(cfg);
    // Find index size with binary search
    find_size(0, 0, 1, [=](int res, uint64_t size)
    {
        if (res < 0)
        {
            this->inode_id = 0;
            this->kv_block_size = 0;
        }
        this->next_free = size;
        cb(res);
    });
}

void kv_db_t::set_config(json11::Json cfg)
{
    this->memory_limit = cfg["kv_memory_limit"].is_null() ? 128*1024*1024 : cfg["kv_memory_limit"].uint64_value();
    this->evict_max_misses = cfg["kv_evict_max_misses"].is_null() ? 10 : cfg["kv_evict_max_misses"].uint64_value();
    this->evict_attempts_per_level = cfg["kv_evict_attempts_per_level"].is_null() ? 3 : cfg["kv_evict_attempts_per_level"].uint64_value();
    this->evict_unused_age = cfg["kv_evict_unused_age"].is_null() ? 1000 : cfg["kv_evict_unused_age"].uint64_value();
    this->cache_max_blocks = this->memory_limit / this->kv_block_size;
    this->allocate_blocks = cfg["kv_allocate_blocks"].uint64_value() ? cfg["kv_allocate_blocks"].uint64_value() : 4;
    this->log_level = !cfg["kv_log_level"].is_null() ? cfg["kv_log_level"].uint64_value() : 1;
}

void kv_db_t::close(std::function<void()> cb)
{
    if (active_ops <= 0)
    {
        closing = false;
        on_close = NULL;
        inode_id = 0;
        next_free = 0;
        kv_block_size = 0;
        ino_block_size = 0;
        immediate_commit = false;
        block_cache.clear();
        known_versions.clear();
        cb();
    }
    else
    {
        closing = true;
        on_close = cb;
    }
}

uint64_t kv_db_t::alloc_block()
{
    // round robin between <allocate_blocks> blocks
    while (allocating_blocks.size() < allocate_blocks)
    {
        allocating_blocks.push_back(UINT64_MAX);
    }
    if (allocating_blocks[allocating_block_pos] == UINT64_MAX)
    {
        // try v0 and only v0 of a new inode block
        while (known_versions[next_free/ino_block_size] != 0)
        {
            next_free += ino_block_size;
        }
        allocating_blocks[allocating_block_pos] = next_free;
        next_free += ino_block_size;
    }
    auto pos = allocating_blocks[allocating_block_pos];
    allocating_blocks[allocating_block_pos] += kv_block_size;
    if (!(allocating_blocks[allocating_block_pos] % ino_block_size))
    {
        // Allow online reconfiguration
        if (allocating_blocks.size() > allocate_blocks)
            allocating_blocks.erase(allocating_blocks.begin()+allocating_block_pos, allocating_blocks.begin()+allocating_block_pos+1);
        else
            allocating_blocks[allocating_block_pos] = UINT64_MAX;
    }
    allocating_block_pos = (allocating_block_pos+1) % allocating_blocks.size();
    return pos;
}

void kv_db_t::clear_allocation_block(uint64_t offset)
{
    // We want to be first when writing the first KV block into a new inode block
    // After it other writers may modify already placed blocks and the version
    // will increase, but they won't use free space in it for new blocks
    if (known_versions[offset/ino_block_size] == 0)
    {
        for (int i = 0; i < allocating_blocks.size(); i++)
        {
            if (allocating_blocks[i]/ino_block_size == offset/ino_block_size)
            {
                allocating_blocks[i] = UINT64_MAX;
            }
        }
    }
}

static bool is_zero(void *buf, int size)
{
    assert(!(size % 8));
    size /= 8;
    uint64_t *ptr = (uint64_t*)buf;
    for (int i = 0; i < size/8; i++)
        if (ptr[i])
            return false;
    return true;
}

// Find approximate index size
// Phase 1: try 2^i-1 for i=0,1,2,... * ino_block_size
// Phase 2: binary search between 2^(N-1)-1 and 2^N-1 * ino_block_size
void kv_db_t::find_size(uint64_t min, uint64_t max, int phase, std::function<void(int, uint64_t)> cb)
{
    if (min == max-1)
    {
        cb(0, max*ino_block_size);
        return;
    }
    if (phase == 1 && min >= KV_INDEX_MAX_SIZE)
    {
        cb(-EFBIG, 0);
        return;
    }
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = inode_id;
    op->offset = (phase == 1 ? min : (min+max)/2) * ino_block_size;
    op->len = kv_block_size;
    if (op->len)
    {
        op->iov.push_back(malloc_or_die(op->len), op->len);
    }
    op->callback = [=](cluster_op_t *op)
    {
        if (op->retval != op->len)
        {
            // error
            cb(op->retval >= 0 ? -EIO : op->retval, 0);
            return;
        }
        known_versions[op->offset / ino_block_size] = op->version;
        if (op->version != 0)
        {
            if (phase == 1)
                find_size((min+1)*2 - 1, 0, 1, cb);
            else
                find_size((min+max)/2, max, 2, cb);
        }
        else
        {
            if (phase == 1)
                find_size((min+1)/2 - 1, min, 2, cb);
            else
                find_size(min, (min+max)/2, 2, cb);
        }
        free(op->iov.buf[0].iov_base);
        delete op;
    };
    cli->execute(op);
}

void kv_db_t::run_continue_update(uint64_t offset)
{
    auto b_it = continue_update.find(offset);
    if (b_it != continue_update.end())
    {
        auto cb = b_it->second;
        continue_update.erase(b_it);
        cb();
    }
}

void kv_db_t::stop_updating(kv_block_t *blk)
{
    assert(blk->updating > 0);
    blk->updating--;
    if (!blk->updating)
        run_continue_update(blk->offset);
}

static void del_block_level(kv_db_t *db, kv_block_t *blk)
{
    db->block_levels.erase((((uint64_t)(db->base_block_level+blk->level) & 0xFFFF) << (64-LEVEL_BITS)) | (blk->offset/db->kv_block_size));
}

static void add_block_level(kv_db_t *db, kv_block_t *blk)
{
    db->block_levels.insert((((uint64_t)(db->base_block_level+blk->level) & 0xFFFF) << (64-LEVEL_BITS)) | (blk->offset/db->kv_block_size));
}

static void invalidate(kv_db_t *db, uint64_t offset, uint64_t version)
{
    if (db->known_versions[offset/db->ino_block_size] < version)
    {
        db->clear_allocation_block(offset);
        auto b_it = db->block_cache.lower_bound(offset/db->ino_block_size * db->ino_block_size);
        while (b_it != db->block_cache.end() && b_it->first/db->ino_block_size == offset/db->ino_block_size)
        {
            if (b_it->second.updating > 0)
            {
                // do not forget blocks during modification
                // but we have to remember the fact that they're not valid anymore
                // because otherwise, if we keep `updating` flag more than just during
                // the write_block() operation, we may end up modifying an outdated
                // version of the block in memory
                b_it->second.invalidated = true;
                b_it++;
            }
            else
            {
                auto blk = &b_it->second;
                del_block_level(db, blk);
                db->block_cache.erase(b_it++);
            }
        }
        db->known_versions[offset/db->ino_block_size] = version;
    }
}

static uint64_t get_max_level(kv_db_t *db)
{
    auto el_it = db->block_levels.end();
    if (el_it == db->block_levels.begin())
    {
        return 0;
    }
    el_it--;
    return (*el_it >> (64-LEVEL_BITS));
}

static void try_evict(kv_db_t *db)
{
    // Evict blocks from cache based on memory limit and block level
    if (db->cache_max_blocks <= 10 || db->block_cache.size() <= db->cache_max_blocks)
    {
        return;
    }
    for (uint64_t evict_level = get_max_level(db); evict_level > 0; evict_level--)
    {
        // Do <evict_attempts_per_level> eviction attempts at random block positions per each block level
        for (int attempt = 0; attempt < db->evict_attempts_per_level; attempt++)
        {
            auto start_it = db->block_levels.lower_bound(evict_level << (64-LEVEL_BITS));
            auto end_it = db->block_levels.lower_bound((evict_level+1) << (64-LEVEL_BITS));
            if (start_it == end_it)
                continue;
            end_it--;
            if (start_it == end_it)
                continue;
            auto random_pos = *start_it + (lrand48() % (*end_it - *start_it));
            auto random_it = db->block_levels.lower_bound(random_pos);
            int misses = 0;
            bool wrapped = false;
            while (db->block_cache.size() > db->cache_max_blocks &&
                (!wrapped || *random_it < random_pos) &&
                (db->evict_max_misses <= 0 || misses < db->evict_max_misses))
            {
                auto b_it = db->block_cache.find((*random_it & NO_LEVEL_MASK) * db->kv_block_size);
                auto blk = &b_it->second;
                if (b_it != db->block_cache.end() && !blk->updating && blk->usage < db->usage_counter)
                {
                    db->block_cache.erase(b_it);
                    db->block_levels.erase(random_it++);
                }
                else
                {
                    random_it++;
                    misses++;
                }
                if (random_it == db->block_levels.end() || (*random_it >> (64-LEVEL_BITS)) > evict_level)
                {
                    if (wrapped)
                        break;
                    random_it = db->block_levels.lower_bound(evict_level << (64-LEVEL_BITS));
                    wrapped = true;
                }
            }
            if (db->block_cache.size() <= db->cache_max_blocks)
            {
                return;
            }
        }
    }
}

static void get_block(kv_db_t *db, uint64_t offset, int cur_level, int recheck_policy, std::function<void(int, bool)> cb)
{
    auto b_it = db->block_cache.find(offset);
    if (b_it != db->block_cache.end() && (recheck_policy == KV_RECHECK_NONE && !b_it->second.invalidated ||
        recheck_policy == KV_RECHECK_LEAF && b_it->second.type != KV_LEAF && !b_it->second.invalidated ||
        b_it->second.updating))
    {
        // Block already in cache, we can proceed
        b_it->second.usage = db->usage_counter;
        cb(0, false);
        return;
    }
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = db->inode_id;
    op->offset = offset;
    op->len = db->kv_block_size;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    op->callback = [=](cluster_op_t *op)
    {
        if (op->retval != op->len)
        {
            // error
            cb(op->retval >= 0 ? -EIO : op->retval, false);
            return;
        }
        if (db->block_cache.find(op->offset) != db->block_cache.end() &&
            db->known_versions[op->offset/db->ino_block_size] == op->version)
        {
            auto blk = &db->block_cache.at(op->offset);
            blk->usage = db->usage_counter;
            cb(0, false);
        }
        else
        {
            invalidate(db, op->offset, op->version);
            try_evict(db);
            auto blk = &db->block_cache[op->offset];
            int err = blk->parse(op->offset, (uint8_t*)op->iov.buf[0].iov_base, op->len);
            if (err == 0)
            {
                blk->level = cur_level;
                blk->usage = db->usage_counter;
                add_block_level(db, blk);
                cb(0, true);
            }
            else
            {
                db->block_cache.erase(op->offset);
                cb(err, false);
            }
        }
        free(op->iov.buf[0].iov_base);
        delete op;
    };
    db->cli->execute(op);
}

void kv_op_t::exec()
{
    if (started)
        return;
    started = true;
    db->active_ops++;
    if (!db->inode_id || db->closing)
    {
        finish(-EINVAL);
        return;
    }
    if (++db->evict_unused_counter >= db->evict_unused_age)
    {
        db->evict_unused_counter = 0;
        db->usage_counter++;
    }
    cur_level = -db->base_block_level;
    if (opcode == KV_LIST)
    {
        path.clear();
        path.push_back((kv_path_t){ .offset = 0 });
    }
    recheck_policy = (opcode == KV_GET ? KV_RECHECK_LEAF : KV_RECHECK_NONE);
    if (opcode == KV_GET || opcode == KV_GET_CACHED)
        get();
    else if (opcode == KV_SET || opcode == KV_DEL)
        update();
    else if (opcode == KV_LIST)
    {
        // Do nothing, next() does everything
    }
    else
        finish(-ENOSYS);
}

void kv_op_t::finish(int res)
{
    this->res = res;
    this->done = true;
    db->active_ops--;
    if (!db->active_ops && db->closing)
        db->close(db->on_close);
    (std::function<void(kv_op_t *)>(callback))(this);
}

void kv_op_t::get()
{
    get_block(db, cur_block, cur_level, recheck_policy, [=](int res, bool updated)
    {
        res = handle_block(res, updated, false);
        if (res == -EAGAIN)
        {
            get();
        }
        else if (res == -ENOTBLK)
        {
            if (cur_block != 0)
            {
                fprintf(stderr, "K/V: Hit empty block %lu while searching\n", cur_block);
                finish(-EILSEQ);
            }
            else
                finish(-ENOENT);
        }
        else if (res < 0)
        {
            finish(res);
        }
        else
        {
            auto blk = &db->block_cache.at(cur_block);
            auto kv_it = blk->data.find(key);
            if (kv_it == blk->data.end())
            {
                finish(-ENOENT);
            }
            else
            {
                this->res = 0;
                this->value = kv_it->second;
                finish(0);
            }
        }
    });
}

int kv_op_t::handle_block(int res, bool updated, bool stop_on_split)
{
    if (res < 0)
    {
        return res;
    }
    this->updated = this->updated || updated;
    auto blk = &db->block_cache.at(cur_block);
    if (opcode != KV_GET && opcode != KV_GET_CACHED)
    {
        // Track the whole path and versions of all blocks during update
        // and recheck parent blocks if their versions change. This is required
        // because we have to handle parallel splits of parent blocks correctly
        assert(path.size() > 0);
        path[path.size()-1].version = db->known_versions[cur_block/db->ino_block_size];
    }
    if (key < blk->key_ge || blk->key_lt.size() && key >= blk->key_lt)
    {
        // We got an unrelated block - recheck the whole chain from the beginning
        // May happen during split:
        // 1) Initially parent P points to block A, A contains [a, b)
        // 2) New block B = [c, b)
        // 3) P = ->A + ->B
        // 4) A = [a, c)
        // We may read P on step (1), get a link to A, and read A on step (4).
        // It will miss data from [c, b).
        // Retry once. If we don't see any updates after retrying - fail with EILSEQ.
        bool fatal = !this->updated && this->retry > 0;
        if (fatal || db->log_level > 0)
        {
            fprintf(stderr, "K/V: %sgot unrelated block %lu: key=%s range=[%s, %s) from=[%s, %s)\n",
                fatal ? "Error: " : "Warning: read/update collision: ",
                cur_block, key.c_str(), blk->key_ge.c_str(), blk->key_lt.c_str(), prev_key_ge.c_str(), prev_key_lt.c_str());
        }
        if (this->updated)
        {
            this->updated = false;
            this->retry = 0;
        }
        else if (this->retry > 0)
        {
            return -EILSEQ;
        }
        else
        {
            this->updated = false;
            this->retry++;
        }
        prev_key_ge = prev_key_lt = "";
        cur_level = -db->base_block_level;
        cur_block = 0;
        if (opcode != KV_GET && opcode != KV_GET_CACHED)
        {
            path.clear();
            path.push_back((kv_path_t){ .offset = 0 });
        }
        this->recheck_policy = KV_RECHECK_ALL;
        return -EAGAIN;
    }
    if (stop_on_split && (blk->type == KV_LEAF_SPLIT || blk->type == KV_INT_SPLIT) &&
        (prev_key_lt == "" || prev_key_lt > blk->right_half))
    {
        return -ECHILD;
    }
    else if ((blk->type == KV_INT_SPLIT || blk->type == KV_LEAF_SPLIT) && key >= blk->right_half)
    {
        cur_block = blk->right_half_block;
        if (opcode != KV_GET && opcode != KV_GET_CACHED)
        {
            assert(path.size() > 0);
            path[path.size()-1].offset = cur_block;
            path[path.size()-1].version = 0;
        }
        prev_key_ge = blk->right_half;
        prev_key_lt = blk->key_lt;
        return -EAGAIN;
    }
    else if (blk->type == KV_LEAF || blk->type == KV_LEAF_SPLIT)
    {
        return 0;
    }
    else
    {
        auto child_it = blk->data.upper_bound(key);
        if (child_it == blk->data.begin())
        {
            fprintf(stderr, "K/V: Internal block %lu misses boundary for %s\n", cur_block, key.c_str());
            return -EILSEQ;
        }
        auto m = child_it == blk->data.end() ? blk->key_lt : child_it->first;
        child_it--;
        if (child_it->second.size() != sizeof(uint64_t))
        {
            fprintf(stderr, "K/V: Internal block %lu reference is not 8 byte long\n", cur_block);
            blk->dump(db->base_block_level);
            return -EILSEQ;
        }
        // Track left and right boundaries which have led us to cur_block
        prev_key_ge = child_it->first;
        prev_key_lt = m;
        cur_level++;
        cur_block = *((uint64_t*)child_it->second.data());
        if (opcode != KV_GET && opcode != KV_GET_CACHED)
        {
            path.push_back((kv_path_t){ .offset = cur_block });
        }
        return -EAGAIN;
    }
    return 0;
}

static std::string find_splitter(kv_db_t *db, kv_block_t *blk)
{
    uint32_t new_size = blk->data_size;
    auto d_it = blk->data.end();
    while (d_it != blk->data.begin() && new_size > db->kv_block_size/2)
    {
        d_it--;
        new_size -= kv_block_t::kv_size(d_it->first, d_it->second);
    }
    assert(d_it != blk->data.begin() && d_it != blk->data.end());
    if (blk->type != KV_LEAF && blk->type != KV_LEAF_SPLIT)
    {
        return d_it->first;
    }
    auto prev_it = std::prev(d_it);
    int i = 0;
    while (i < d_it->first.size() && i < prev_it->first.size() && d_it->first[i] == prev_it->first[i])
    {
        i++;
    }
    auto separator = i < d_it->first.size() ? d_it->first.substr(0, i+1) : d_it->first;
    return separator;
}

static void write_block(kv_db_t *db, kv_block_t *blk, std::function<void(int)> cb)
{
    if (blk->invalidated)
    {
        // Cancel all writes into the same inode block
        auto b_it = db->continue_write.find(blk->offset/db->ino_block_size);
        while (b_it != db->continue_write.end() && b_it->first == blk->offset/db->ino_block_size)
        {
            auto cont = b_it->second;
            db->continue_write.erase(b_it++);
            cont.cb(-EINTR);
        }
        cb(-EINTR);
        return;
    }
    auto & new_version = db->new_versions[blk->offset/db->ino_block_size];
    if (new_version != 0)
    {
        // Wait if block is being modified
        db->continue_write.emplace(blk->offset/db->ino_block_size, (kv_continue_write_t){ .blk = blk, .cb = cb });
        return;
    }
    new_version = 1+db->known_versions[blk->offset/db->ino_block_size];
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = db->inode_id;
    op->offset = blk->offset;
    op->version = new_version;
    op->len = db->kv_block_size;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    if (!blk->serialize((uint8_t*)op->iov.buf[0].iov_base, op->len))
    {
        blk->dump(db->base_block_level);
        uint64_t old_size = blk->data_size;
        blk->set_data_size();
        fprintf(stderr, "K/V: block %lu (ptr=%lx) grew too large: tracked %lu, but real is %u bytes\n",
            blk->offset, (uint64_t)blk, old_size, blk->data_size);
        abort();
        return;
    }
    op->callback = [db, blk, cb](cluster_op_t *op)
    {
        db->new_versions.erase(blk->offset/db->ino_block_size);
        free(op->iov.buf[0].iov_base);
        int res = op->retval == op->len ? 0 : (op->retval > 0 ? -EIO : op->retval);
        if (res == 0)
        {
            blk->invalidated = false;
            db->known_versions[blk->offset/db->ino_block_size] = op->version;
            auto b_it = db->continue_write.find(blk->offset/db->ino_block_size);
            if (b_it != db->continue_write.end())
            {
                auto cont = b_it->second;
                db->continue_write.erase(b_it++);
                write_block(db, cont.blk, cont.cb);
            }
        }
        else
        {
            // Cancel all writes into the same inode block
            auto b_it = db->continue_write.find(blk->offset/db->ino_block_size);
            while (b_it != db->continue_write.end() && b_it->first == blk->offset/db->ino_block_size)
            {
                auto cont = b_it->second;
                db->continue_write.erase(b_it++);
                cont.cb(res);
            }
        }
        delete op;
        if (res < 0 || db->immediate_commit)
        {
            cb(res);
        }
        else
        {
            op = new cluster_op_t;
            op->opcode = OSD_OP_SYNC;
            op->callback = [cb](cluster_op_t *op)
            {
                auto res = op->retval;
                delete op;
                cb(res);
            };
            db->cli->execute(op);
        }
    };
    db->cli->execute(op);
}

static kv_block_t *create_new_block(kv_db_t *db, kv_block_t *old_blk, const std::string & separator,
    const std::string & added_key, const std::string & added_value, bool right)
{
    auto new_offset = db->alloc_block();
    auto blk = &db->block_cache[new_offset];
    blk->usage = db->usage_counter;
    blk->level = old_blk->level;
    blk->type = old_blk->type == KV_LEAF_SPLIT || old_blk->type == KV_LEAF ? KV_LEAF : KV_INT;
    blk->offset = new_offset;
    blk->updating++;
    blk->key_ge = right ? separator : old_blk->key_ge;
    blk->key_lt = right ? old_blk->key_lt : separator;
    blk->data.insert(right ? old_blk->data.lower_bound(separator) : old_blk->data.begin(),
        right ? old_blk->data.end() : old_blk->data.lower_bound(separator));
    if ((added_key >= separator) == right)
        blk->data[added_key] = added_value;
    blk->set_data_size();
    add_block_level(db, blk);
    return blk;
}

static void write_new_block(kv_db_t *db, kv_block_t *blk, std::function<void(int, kv_block_t *)> cb)
{
    write_block(db, blk, [=](int res)
    {
        if (res == -EINTR)
        {
            // CAS failure => re-read, then, if not zero, find position again and retry
            db->clear_allocation_block(blk->offset);
            cluster_op_t *op = new cluster_op_t;
            op->opcode = OSD_OP_READ;
            op->inode = db->inode_id;
            op->offset = blk->offset;
            op->len = db->kv_block_size;
            op->iov.push_back(malloc_or_die(op->len), op->len);
            op->callback = [=](cluster_op_t *op)
            {
                if (op->retval != op->len)
                {
                    // Read error => free the new unreferenced block and die
                    del_block_level(db, blk);
                    db->block_cache.erase(blk->offset);
                    cb(op->retval >= 0 ? -EIO : op->retval, NULL);
                    return;
                }
                invalidate(db, op->offset, op->version);
                if (is_zero(op->iov.buf[0].iov_base, db->kv_block_size))
                {
                    // OK, block is still empty, but the version apparently changed
                    blk->invalidated = false;
                    write_new_block(db, blk, cb);
                }
                else
                {
                    // Block is already occupied => place again
                    auto old_offset = blk->offset;
                    auto new_offset = db->alloc_block();
                    del_block_level(db, blk);
                    std::swap(db->block_cache[new_offset], db->block_cache[old_offset]);
                    db->block_cache.erase(old_offset);
                    auto new_blk = &db->block_cache[new_offset];
                    new_blk->offset = new_offset;
                    new_blk->invalidated = false;
                    add_block_level(db, new_blk);
                    write_new_block(db, new_blk, cb);
                }
            };
            db->cli->execute(op);
        }
        else if (res != 0)
        {
            // Other failure => free the new unreferenced block and die
            del_block_level(db, blk);
            db->block_cache.erase(blk->offset);
            cb(res > 0 ? -EIO : res, NULL);
        }
        else
        {
            // OK
            cb(0, blk);
        }
    });
}

static void clear_block(kv_db_t *db, kv_block_t *blk, uint64_t version, std::function<void(int)> cb)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = db->inode_id;
    op->offset = blk->offset;
    op->version = version;
    op->len = db->kv_block_size;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    memset(op->iov.buf[0].iov_base, 0, op->len);
    kv_stored_block_t *sb = (kv_stored_block_t *)op->iov.buf[0].iov_base;
    // Write non-zero data to not get mistaken when finding index size with binary search
    sb->magic = KV_BLOCK_MAGIC;
    sb->block_size = db->kv_block_size;
    sb->type = KV_EMPTY;
    op->callback = [cb](cluster_op_t *op)
    {
        free(op->iov.buf[0].iov_base);
        auto res = op->retval == op->len ? 0 : (op->retval >= 0 ? -EIO : op->retval);
        delete op;
        cb(res);
    };
    del_block_level(db, blk);
    db->block_cache.erase(blk->offset);
    db->cli->execute(op);
}

void kv_op_t::update()
{
    if (opcode == KV_SET && kv_block_t::kv_size(key, value) > (db->kv_block_size-sizeof(kv_stored_block_t)) / 4)
    {
        // New item is too large for this B-Tree
        finish(-EINVAL);
        return;
    }
    prev_key_ge = prev_key_lt = "";
    cur_level = -db->base_block_level;
    cur_block = 0;
    path.clear();
    path.push_back((kv_path_t){ .offset = 0 });
    // The beginning is (almost) the same as in get(). First we find path to the item
    update_find();
}

void kv_op_t::update_find()
{
    get_block(db, cur_block, cur_level, recheck_policy, [=, checked_block = cur_block](int res, bool updated)
    {
        res = handle_block(res, updated, true);
        if (res == -EAGAIN)
        {
            update_find();
        }
        else if (res == -ENOTBLK)
        {
            if (opcode == KV_SET)
            {
                // Check CAS callback
                if (cas_cb && !cas_cb(-ENOENT, ""))
                    finish(-EAGAIN);
                else
                    create_root();
            }
            else if (cur_block == 0)
                finish(-ENOENT);
            else
                fprintf(stderr, "K/V: Hit empty block %lu while searching\n", cur_block);
        }
        else if (res == -ECHILD)
        {
            resume_split();
        }
        else if (res < 0)
        {
            finish(res);
        }
        else
        {
            update_block(path.size()-1, opcode == KV_DEL, key, value, [=](int res)
            {
                finish(res);
            });
        }
    });
}

void kv_op_t::create_root()
{
    // if the block does not exist (is empty) - it should be the root block.
    // in this case we just create a new root leaf block.
    // if a referenced non-root block is empty, we just return an error.
    if (cur_block != 0 || db->next_free != 0)
    {
        fprintf(stderr, "K/V: create_root called with non-empty DB (cur_block=%lu)\n", cur_block);
        finish(-EILSEQ);
        return;
    }
    auto new_offset = db->alloc_block();
    assert(new_offset == 0);
    auto blk = &db->block_cache[0];
    blk->usage = db->usage_counter;
    blk->level = -db->base_block_level;
    blk->type = KV_LEAF;
    blk->offset = new_offset;
    blk->data[key] = value;
    blk->set_data_size();
    add_block_level(db, blk);
    blk->updating++;
    write_block(db, blk, [=](int res)
    {
        if (res == -EINTR)
        {
            auto blk_offset = blk->offset;
            del_block_level(db, blk);
            db->block_cache.erase(blk_offset);
            db->run_continue_update(blk_offset);
            update();
        }
        else
        {
            db->stop_updating(blk);
            finish(res);
        }
    });
}

void kv_op_t::resume_split()
{
    // We hit a block which we started to split, but didn't finish splitting
    // Generally it shouldn't happen, but it MAY happen if a K/V client dies
    // In this case we want to finish the split and retry update from the beginning
    if (path.size() == 1)
    {
        // It shouldn't be the root block because we don't split it via INT_SPLIT/LEAF_SPLIT
        fprintf(stderr, "K/V: resume_split at root item (cur_block=%lu)\n", cur_block);
        finish(-EILSEQ);
        return;
    }
    auto blk = &db->block_cache.at(cur_block);
    update_block(
        path.size()-2, false, blk->right_half,
        std::string((char*)&blk->right_half_block, sizeof(blk->right_half_block)),
        [=](int res)
        {
            if (res < 0)
            {
                finish(res);
                return;
            }
            update();
        }
    );
}

void kv_op_t::update_block(int path_pos, bool is_delete, const std::string & key,
    const std::string & value, std::function<void(int)> cb)
{
    auto blk_it = db->block_cache.find(path[path_pos].offset);
    if (blk_it == db->block_cache.end())
    {
        // Block is not in cache anymore, recheck
        db->run_continue_update(path[path_pos].offset);
        update();
        return;
    }
    auto blk = &blk_it->second;
    auto block_ver = path[path_pos].version;
    if (blk->updating)
    {
        // Wait if block is being modified
        db->continue_update.emplace(blk->offset, [=]() { update_block(path_pos, is_delete, key, value, cb); });
        return;
    }
    if (db->known_versions[blk->offset/db->ino_block_size] != block_ver)
    {
        // Recheck if block was modified in the meantime
        db->run_continue_update(blk->offset);
        update();
        return;
    }
    uint32_t rm_size = 0;
    auto d_it = blk->data.find(key);
    if (d_it != blk->data.end())
    {
        if (!is_delete && d_it->second == value)
        {
            // Nothing to do
            db->run_continue_update(blk->offset);
            cb(0);
            return;
        }
        rm_size = kv_block_t::kv_size(d_it->first, d_it->second);
    }
    else if (is_delete)
    {
        // Nothing to do
        db->run_continue_update(blk->offset);
        cb(0);
        return;
    }
    if (cas_cb && path_pos == path.size()-1 && !cas_cb(d_it != blk->data.end() ? 0 : -ENOENT, d_it != blk->data.end() ? d_it->second : ""))
    {
        // CAS failure
        db->run_continue_update(blk->offset);
        cb(-EAGAIN);
        return;
    }
    // This condition means this is a block split during previous updates
    // Its parent is already updated because prev_key_lt <= blk->right_half
    // That means we can safely remove the "split reference"
    assert(!blk->change_type);
    if (blk->type == KV_LEAF_SPLIT || blk->type == KV_INT_SPLIT)
    {
        if (prev_key_lt != "" && prev_key_lt <= blk->right_half ||
            // We can't check it for sure for parent blocks because we don't track their prev_key_lt/ge
            path_pos < path.size()-1)
            blk->change_type = KV_CH_CLEAR_RIGHT;
        else
        {
            blk->dump(0);
            // Should not happen - we should have resumed the split
            fprintf(stderr, "K/V: attempt to write into a block %lu instead of resuming the split (got here from %s..%s)\n",
                blk->offset, prev_key_ge.c_str(), prev_key_lt.c_str());
            abort();
        }
    }
    blk->updating++;
    if (is_delete || (blk->data_size + kv_block_t::kv_size(key, value) - rm_size) < db->kv_block_size)
    {
        // New item fits.
        // No need to split the block => just modify and write it
        if (is_delete)
        {
            blk->change_type |= KV_CH_DEL;
            blk->change_key = key;
        }
        else
        {
            blk->change_type |= (d_it != blk->data.end() ? KV_CH_UPD : KV_CH_ADD);
            blk->change_key = key;
            blk->change_value = value;
        }
        write_block(db, blk, [=](int res)
        {
            if (res < 0)
            {
                auto blk_offset = blk->offset;
                del_block_level(db, blk);
                db->block_cache.erase(blk_offset);
                db->run_continue_update(blk_offset);
            }
            else
            {
                blk->apply_change();
                db->stop_updating(blk);
            }
            if (res == -EINTR)
            {
                update();
            }
            else
            {
                // FIXME We may want to merge blocks at some point, in that case we should:
                // - read both merged blocks
                // - add "merged into XX" reference to the second block and write it out
                // - add entries from the second block to the first one and write it out
                // - remove the reference from the parent block to the second block
                // - zero out the second block
                cb(res);
            }
        });
        return;
    }
    // New item doesn't fit. The most interesting case.
    // Write the right half into a new block
    auto separator = find_splitter(db, blk);
    auto orig_right_blk = create_new_block(db, blk, separator, key, value, true);
    write_new_block(db, orig_right_blk, [=](int res, kv_block_t *right_blk)
    {
        if (res < 0)
        {
            blk->cancel_change();
            db->stop_updating(blk);
            cb(res);
            return;
        }
        if (!blk->offset)
        {
            // Split the root block
            // Write the left half into a new block
            auto orig_left_blk = create_new_block(db, blk, separator, key, value, false);
            write_new_block(db, orig_left_blk, [=](int res, kv_block_t *left_blk)
            {
                if (res < 0)
                {
                    // FIXME: clear_block left_blk
                    blk->cancel_change();
                    db->stop_updating(right_blk);
                    db->stop_updating(blk);
                    cb(res);
                    return;
                }
                // Write references to halves into the new root block
                auto new_root = new kv_block_t;
                new_root->offset = 0;
                new_root->usage = db->usage_counter;
                new_root->type = KV_INT;
                new_root->level = blk->level-1;
                new_root->change_type = 0;
                new_root->data.clear();
                new_root->data[""] = std::string((char*)&left_blk->offset, sizeof(left_blk->offset));
                new_root->data[separator] = std::string((char*)&right_blk->offset, sizeof(right_blk->offset));
                new_root->set_data_size();
                new_root->updating++;
                if (blk->invalidated)
                {
                    new_root->invalidated = true;
                }
                write_block(db, new_root, [=](int write_res)
                {
                    if (write_res < 0)
                    {
                        auto blk_offset = blk->offset;
                        del_block_level(db, blk);
                        db->block_cache.erase(blk_offset);
                        db->run_continue_update(blk_offset);
                        clear_block(db, left_blk, 0, [=, left_offset = left_blk->offset](int res)
                        {
                            if (res < 0)
                                fprintf(stderr, "Failed to clear unreferenced block %lu: %s (code %d)\n", left_offset, strerror(-res), res);
                            clear_block(db, right_blk, 0, [=, right_offset = right_blk->offset](int res)
                            {
                                if (res < 0)
                                    fprintf(stderr, "Failed to clear unreferenced block %lu: %s (code %d)\n", right_offset, strerror(-res), res);
                                // CAS failure - zero garbage left_blk and right_blk and retry from the beginning
                                if (write_res == -EINTR)
                                    update();
                                else
                                    cb(write_res);
                            });
                        });
                    }
                    else
                    {
                        std::swap(db->block_cache[0], *new_root);
                        db->base_block_level = -new_root->level;
                        db->stop_updating(left_blk);
                        db->stop_updating(right_blk);
                        db->stop_updating(&db->block_cache[0]);
                        cb(0);
                    }
                    delete new_root;
                });
            });
        }
        else
        {
            if (path_pos == 0)
            {
                // Block number zero should always be the root block
                fprintf(stderr, "K/V: root block is not 0, but %lu\n", cur_block);
                cb(-EILSEQ);
                return;
            }
            // Split a non-root block
            blk->change_type = (blk->change_type & ~KV_CH_CLEAR_RIGHT) | KV_CH_SPLIT;
            blk->change_rh = separator;
            blk->change_rh_block = right_blk->offset;
            if (key < separator)
            {
                blk->change_type |= (blk->data.find(key) != blk->data.end() ? KV_CH_UPD : KV_CH_ADD);
                blk->change_key = key;
                blk->change_value = value;
            }
            write_block(db, blk, [=](int write_res)
            {
                if (write_res < 0)
                {
                    auto blk_offset = blk->offset;
                    del_block_level(db, blk);
                    db->block_cache.erase(blk_offset);
                    db->run_continue_update(blk_offset);
                    clear_block(db, right_blk, 0, [=, right_offset = right_blk->offset](int res)
                    {
                        if (res < 0)
                            fprintf(stderr, "Failed to clear unreferenced block %lu: %s (code %d)\n", right_offset, strerror(-res), res);
                        // CAS failure - zero garbage right_blk and retry from the beginning
                        if (write_res == -EINTR)
                            update();
                        else
                            cb(write_res);
                    });
                }
                else
                {
                    blk->apply_change();
                    db->stop_updating(blk);
                    db->stop_updating(right_blk);
                    // Add a reference to the parent block
                    // Do not cleanup anything on failure because stored right_blk is already referenced
                    update_block(
                        path_pos-1, false, separator,
                        std::string((char*)&right_blk->offset, sizeof(right_blk->offset)),
                        cb
                    );
                }
            });
        }
    });
}

void kv_op_t::next()
{
    if (opcode != KV_LIST || !started || done)
    {
        return;
    }
    get_block(db, cur_block, cur_level, recheck_policy, [=](int res, bool updated)
    {
        next_handle_block(res, updated);
    });
}

void kv_op_t::next_handle_block(int res, bool updated)
{
    res = handle_block(res, updated, false);
    if (res == -EAGAIN)
    {
        next();
    }
    else if (res == -ENOTBLK)
    {
        if (cur_block == 0)
            finish(-ENOENT);
        else
        {
            fprintf(stderr, "K/V: Hit empty block %lu while searching\n", cur_block);
            finish(-EILSEQ);
        }
    }
    else if (res < 0)
    {
        finish(res);
    }
    else
    {
        // OK, leaf block found
        recheck_policy = KV_RECHECK_NONE;
        next_get();
    }
}

void kv_op_t::next_get()
{
    auto blk = &db->block_cache.at(cur_block);
    auto kv_it = blk->data.lower_bound(key);
    if (skip_equal && kv_it != blk->data.end() && kv_it->first == key)
    {
        kv_it++;
    }
    if (kv_it != blk->data.end())
    {
        // Send this item
        assert(blk->type == KV_LEAF || blk->type == KV_LEAF_SPLIT);
        this->res = 0;
        this->key = kv_it->first;
        this->value = kv_it->second;
        skip_equal = true;
        callback(this);
    }
    // Find next block
    else if (blk->type == KV_LEAF_SPLIT)
    {
        // Left half finished, go to the right
        recheck_policy = KV_RECHECK_LEAF;
        key = blk->right_half;
        skip_equal = false;
        prev_key_ge = blk->right_half;
        prev_key_lt = blk->key_lt;
        path[path.size()-1].offset = cur_block = blk->right_half_block;
        next();
    }
    else
    {
        next_go_up();
    }
}

void kv_op_t::next_go_up()
{
    auto blk = &db->block_cache.at(cur_block);
    while (true)
    {
        if (blk->key_lt == "" || path.size() <= 1)
        {
            // End of the listing
            finish(-ENOENT);
            return;
        }
        if (blk->key_lt != "" && blk->key_lt > key)
        {
            // Block can be updated and key_lt may be lower. Don't rewind it
            key = blk->key_lt;
        }
        skip_equal = false;
        path.pop_back();
        cur_level--;
        cur_block = path[path.size()-1].offset;
        recheck_policy = KV_RECHECK_LEAF;
        // Check if we can resume listing from the next key
        auto pb_it = db->block_cache.find(cur_block);
        if (pb_it == db->block_cache.end())
        {
            // Block is absent in cache, recheck from the beginning
            prev_key_ge = prev_key_lt = "";
            cur_level = -db->base_block_level;
            cur_block = 0;
            path.clear();
            path.push_back((kv_path_t){ .offset = 0 });
            next();
            return;
        }
        blk = &pb_it->second;
        // Go down if block did not end
        if (blk->key_lt == "" || key < blk->key_lt)
        {
            prev_key_ge = blk->key_ge;
            prev_key_lt = blk->key_lt;
            next_handle_block(0, false);
            return;
        }
    }
}

kv_dbw_t::kv_dbw_t(cluster_client_t *cli)
{
    db = new kv_db_t();
    db->cli = cli;
}

kv_dbw_t::~kv_dbw_t()
{
    delete db;
}

void kv_dbw_t::open(inode_t inode_id, json11::Json cfg, std::function<void(int)> cb)
{
    db->open(inode_id, cfg, cb);
}

void kv_dbw_t::set_config(json11::Json cfg)
{
    db->set_config(cfg);
}

uint64_t kv_dbw_t::get_size()
{
    return db->next_free;
}

void kv_dbw_t::close(std::function<void()> cb)
{
    db->close(cb);
}

void kv_dbw_t::get(const std::string & key, std::function<void(int res, const std::string & value)> cb, bool cached)
{
    auto *op = new kv_op_t;
    op->db = db;
    op->opcode = cached ? KV_GET_CACHED : KV_GET;
    op->key = key;
    op->callback = [cb](kv_op_t *op)
    {
        cb(op->res, op->value);
        delete op;
    };
    op->exec();
}

void kv_dbw_t::set(const std::string & key, const std::string & value, std::function<void(int res)> cb,
    std::function<bool(int res, const std::string & value)> cas_compare)
{
    auto *op = new kv_op_t;
    op->db = db;
    op->opcode = KV_SET;
    op->key = key;
    op->value = value;
    if (cas_compare)
    {
        op->cas_cb = cas_compare;
    }
    op->callback = [cb](kv_op_t *op)
    {
        cb(op->res);
        delete op;
    };
    op->exec();
}

void kv_dbw_t::del(const std::string & key, std::function<void(int res)> cb,
    std::function<bool(int res, const std::string & value)> cas_compare)
{
    auto *op = new kv_op_t;
    op->db = db;
    op->opcode = KV_DEL;
    op->key = key;
    if (cas_compare)
    {
        op->cas_cb = cas_compare;
    }
    op->callback = [cb](kv_op_t *op)
    {
        cb(op->res);
        delete op;
    };
    op->exec();
}

void* kv_dbw_t::list_start(const std::string & start)
{
    auto *op = new kv_op_t;
    op->db = db;
    op->opcode = KV_LIST;
    op->key = start;
    op->exec();
    return op;
}

void kv_dbw_t::list_next(void *handle, std::function<void(int res, const std::string & key, const std::string & value)> cb)
{
    kv_op_t *op = (kv_op_t*)handle;
    if (cb)
    {
        op->callback = [cb](kv_op_t *op)
        {
            cb(op->res, op->key, op->value);
        };
    }
    op->next();
}

void kv_dbw_t::list_close(void *handle)
{
    kv_op_t *op = (kv_op_t*)handle;
    delete op;
}
