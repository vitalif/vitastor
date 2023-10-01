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

#define KV_GET 1
#define KV_SET 2
#define KV_DEL 3
#define KV_LIST 4

#define KV_INT 1
#define KV_INT_SPLIT 2
#define KV_LEAF 3
#define KV_LEAF_SPLIT 4
#define KV_EMPTY 5

#define KV_RECHECK_NONE 0
#define KV_RECHECK_LEAF 1
#define KV_RECHECK_ALL 2
#define KV_RECHECK_RELOAD 3

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
    // only new version during modification is saved here
    uint64_t new_version;
    bool updating;
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

    void set_data_size();
    static int kv_size(const std::string & key, const std::string & value);
    int parse(uint8_t *data, int size);
    bool serialize(uint8_t *data, int size);
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

struct kv_db_t
{
    cluster_client_t *cli = NULL;

    inode_t inode_id = 0;
    uint64_t next_free = 0;
    uint32_t kv_block_size = 0;
    uint32_t ino_block_size = 0;
    bool immediate_commit = false;
    uint64_t cache_max_blocks = 0;

    int base_block_level = 0;
    int usage_counter = 1;
    std::set<uint64_t> block_levels;
    std::map<uint64_t, kv_block_t> block_cache;
    std::map<uint64_t, uint64_t> known_versions;
    std::multimap<uint64_t, std::function<void()>> continue_update;

    bool closing = false;
    int active_ops = 0;
    std::function<void()> on_close;

    void open(inode_t inode_id, uint32_t kv_block_size, std::function<void(int)> cb);
    void close(std::function<void()> cb);

    void find_size(uint64_t min, uint64_t max, int phase, std::function<void(int, uint64_t)> cb);
    void stop_updating(kv_block_t *blk);
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
    std::vector<uint64_t> path;
    bool updated = false;
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

int kv_block_t::parse(uint8_t *data, int size)
{
    kv_stored_block_t *blk = (kv_stored_block_t *)data;
    if (blk->magic == 0 || blk->type == KV_EMPTY)
    {
        // empty block
        return -ENOTBLK;
    }
    if (blk->magic != KV_BLOCK_MAGIC || blk->block_size != size ||
        !blk->type || blk->type > KV_LEAF_SPLIT || blk->items > KV_BLOCK_MAX_ITEMS)
    {
        // invalid block
        return -EILSEQ;
    }
    this->type = blk->type;
    this->new_version = 0;
    int pos = blk->data - data;
    this->key_ge = read_string(data, size, &pos);
    if (pos < 0)
        return -EILSEQ;
    this->key_lt = read_string(data, size, &pos);
    if (pos < 0)
        return -EILSEQ;
    if (this->type == KV_INT_SPLIT || this->type == KV_LEAF_SPLIT)
    {
        this->right_half = read_string(data, size, &pos);
        if (pos < 0)
            return -EILSEQ;
        if (pos+8 > size)
            return -EILSEQ;
        this->right_half_block = *(uint64_t*)(data+pos);
        pos += 8;
    }
    for (int i = 0; i < blk->items; i++)
    {
        auto key = read_string(data, size, &pos);
        if (pos < 0)
            return -EILSEQ;
        auto value = read_string(data, size, &pos);
        if (pos < 0)
            return -EILSEQ;
        this->data[key] = value;
    }
    this->data_size = pos;
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

bool kv_block_t::serialize(uint8_t *data, int size)
{
    kv_stored_block_t *blk = (kv_stored_block_t *)data;
    blk->magic = KV_BLOCK_MAGIC;
    blk->block_size = size;
    blk->type = this->type;
    blk->items = this->data.size();
    int pos = blk->data - data;
    if (!write_string(data, size, &pos, key_ge))
        return false;
    if (!write_string(data, size, &pos, key_lt))
        return false;
    if (this->type == KV_INT_SPLIT || this->type == KV_LEAF_SPLIT)
    {
        if (!write_string(data, size, &pos, right_half))
            return false;
        if (pos+8 > size)
            return false;
        *(uint64_t*)(data+pos) = right_half_block;
        pos += 8;
    }
    for (auto & kv: this->data)
    {
        if (!write_string(data, size, &pos, kv.first) ||
            !write_string(data, size, &pos, kv.second))
            return false;
    }
    return true;
}

void kv_db_t::open(inode_t inode_id, uint32_t kv_block_size, std::function<void(int)> cb)
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

// Phase 1: try 2^i-1 for i=0,1,2,...
// Phase 2: binary search between 2^(N-1)-1 and 2^N-1
void kv_db_t::find_size(uint64_t min, uint64_t max, int phase, std::function<void(int, uint64_t)> cb)
{
    if (min == max-1)
    {
        cb(0, max*kv_block_size);
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
    op->offset = (phase == 1 ? min : (min+max)/2) * kv_block_size;
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
        if (!is_zero(op->iov.buf[0].iov_base, kv_block_size))
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

void kv_db_t::stop_updating(kv_block_t *blk)
{
    blk->updating = false;
    auto b_it = continue_update.find(blk->offset);
    while (b_it != continue_update.end() && b_it->first == blk->offset)
    {
        auto cb = b_it->second;
        continue_update.erase(b_it++);
        cb();
    }
}

static void del_block_level(kv_db_t *db, kv_block_t *blk)
{
    db->block_levels.erase((((uint64_t)(db->base_block_level+blk->level) & 0xFFFF) << 48) | (blk->offset/db->kv_block_size));
}

static void add_block_level(kv_db_t *db, kv_block_t *blk)
{
    db->block_levels.insert((((uint64_t)(db->base_block_level+blk->level) & 0xFFFF) << 48) | (blk->offset/db->kv_block_size));
}

static void invalidate(kv_db_t *db, uint64_t offset, uint64_t version)
{
    if (db->known_versions[offset/db->ino_block_size] != version)
    {
        auto b_it = db->block_cache.lower_bound(offset/db->ino_block_size * db->ino_block_size);
        while (b_it != db->block_cache.end() && b_it->first/db->ino_block_size == offset/db->ino_block_size)
        {
            if (b_it->second.new_version != 0 || b_it->second.updating)
            {
                // do not forget blocks during modification
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

static void get_block(kv_db_t *db, uint64_t offset, int cur_level, int recheck_policy, std::function<void(int, bool)> cb)
{
    // FIXME: Evict blocks from cache based on memory limit and block level
    auto b_it = db->block_cache.find(offset);
    if (b_it != db->block_cache.end() && (recheck_policy == KV_RECHECK_NONE ||
        recheck_policy == KV_RECHECK_LEAF && b_it->second.type != KV_LEAF ||
        // block is being modified
        b_it->second.updating || b_it->second.new_version != 0))
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
        if (db->known_versions[op->offset/db->ino_block_size] == op->version &&
            db->block_cache.find(op->offset) != db->block_cache.end())
        {
            auto blk = &db->block_cache.at(op->offset);
            blk->usage = db->usage_counter;
            cb(0, true);
        }
        else
        {
            invalidate(db, op->offset, op->version);
            auto blk = &db->block_cache[op->offset];
            int err = blk->parse((uint8_t*)op->iov.buf[0].iov_base, op->len);
            if (err == 0)
            {
                blk->offset = op->offset;
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
    if (!db->inode_id || db->closing)
    {
        db->active_ops++;
        finish(-EINVAL);
        return;
    }
    if (started)
        return;
    db->active_ops++;
    started = true;
    cur_level = -db->base_block_level;
    if (opcode == KV_GET)
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
            finish(cur_block == 0 ? -ENOENT : -EILSEQ);
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
        if (!this->updated)
            return -EILSEQ;
        prev_key_ge = prev_key_lt = "";
        cur_level = -db->base_block_level;
        cur_block = 0;
        this->recheck_policy = KV_RECHECK_ALL;
        this->updated = false;
        return -EAGAIN;
    }
    if (stop_on_split && (blk->type == KV_LEAF_SPLIT || blk->type == KV_INT_SPLIT) &&
        prev_key_lt > blk->right_half)
    {
        return -ECHILD;
    }
    else if ((blk->type == KV_INT_SPLIT || blk->type == KV_LEAF_SPLIT) && key >= blk->right_half)
    {
        cur_block = blk->right_half_block;
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
            return -EILSEQ;
        }
        auto m = child_it == blk->data.end() ? "" : child_it->first;
        child_it--;
        if (child_it->second.size() != sizeof(uint64_t))
        {
            return -EILSEQ;
        }
        // Track left and right boundaries which have led us to cur_block
        prev_key_ge = child_it->first;
        prev_key_lt = m;
        cur_level++;
        cur_block = *((uint64_t*)child_it->second.data());
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
    blk->new_version = 1+db->known_versions[blk->offset/db->ino_block_size];
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = db->inode_id;
    op->offset = blk->offset;
    op->version = blk->new_version;
    op->len = db->kv_block_size;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    if (!blk->serialize((uint8_t*)op->iov.buf[0].iov_base, op->len))
    {
        fprintf(stderr, "BUG: failed to serialize block %lu\n", blk->offset);
        abort();
        return;
    }
    op->callback = [db, blk, cb](cluster_op_t *op)
    {
        free(op->iov.buf[0].iov_base);
        int res = op->retval;
        if (res == op->len)
        {
            res = 0;
            blk->new_version = 0;
            db->known_versions[op->offset/db->ino_block_size] = op->version;
        }
        else if (res >= 0)
            res = -EIO;
        delete op;
        if (res < 0 || db->immediate_commit)
            cb(res);
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

static kv_block_t *create_new_block(kv_db_t *db, kv_block_t *old_blk, std::string separator, bool right)
{
    auto new_offset = db->next_free;
    db->next_free += db->kv_block_size;
    auto blk = &db->block_cache[new_offset];
    blk->usage = db->usage_counter;
    blk->level = old_blk->level;
    blk->type = old_blk->type;
    blk->offset = new_offset;
    blk->key_ge = right ? separator : old_blk->key_ge;
    blk->key_lt = right ? old_blk->key_lt : separator;
    blk->data.insert(right ? old_blk->data.lower_bound(separator) : old_blk->data.begin(),
        right ? old_blk->data.end() : old_blk->data.lower_bound(separator));
    blk->set_data_size();
    add_block_level(db, blk);
    return blk;
}

static void write_new_block(kv_db_t *db, kv_block_t *blk, std::function<void(int)> cb)
{
    write_block(db, blk, [=](int res)
    {
        if (res == -EINTR)
        {
            // CAS failure => re-read, then, if not zero, find position again and retry
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
                    // Failure => free the new unreferenced block and die
                    del_block_level(db, blk);
                    db->block_cache.erase(blk->offset);
                    cb(op->retval >= 0 ? -EIO : op->retval);
                    return;
                }
                invalidate(db, op->offset, op->version);
                if (is_zero(op->iov.buf[0].iov_base, db->kv_block_size))
                {
                    // OK, block is still empty, but the version apparently changed
                    write_new_block(db, blk, cb);
                }
                else
                {
                    // Block is already occupied => place again
                    auto new_offset = db->next_free;
                    db->next_free += db->kv_block_size;
                    db->block_cache[new_offset] = std::move(db->block_cache[blk->offset]);
                    auto new_blk = &db->block_cache[new_offset];
                    *new_blk = *blk;
                    new_blk->offset = new_offset;
                    db->block_cache.erase(blk->offset);
                    del_block_level(db, blk);
                    add_block_level(db, new_blk);
                    write_new_block(db, new_blk, cb);
                }
                free(op->iov.buf[0].iov_base);
                delete op;
            };
            db->cli->execute(op);
        }
        else if (res != 0)
        {
            // Other failure => free the new unreferenced block and die
            del_block_level(db, blk);
            db->block_cache.erase(blk->offset);
            cb(res > 0 ? -EIO : res);
        }
        else
        {
            // OK
            cb(0);
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
    // The beginning is (almost) the same as in kv_get(). First we find path to the item
    update_find();
}

void kv_op_t::update_find()
{
    if (!cur_block)
    {
        path.clear();
    }
    path.push_back(cur_block);
    get_block(db, cur_block, cur_level, recheck_policy, [=](int res, bool updated)
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
            else
                finish(cur_block == 0 ? -ENOENT : -EILSEQ);
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
        finish(-EILSEQ);
        return;
    }
    db->next_free += db->kv_block_size;
    auto blk = &db->block_cache[0];
    blk->usage = db->usage_counter;
    blk->level = -db->base_block_level;
    blk->type = KV_LEAF;
    blk->offset = 0;
    blk->data[key] = value;
    blk->set_data_size();
    add_block_level(db, blk);
    write_block(db, blk, [=](int res)
    {
        if (res == -EINTR)
            update();
        else
            finish(res);
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

void kv_op_t::update_block(int path_pos, bool is_delete, const std::string & key, const std::string & value, std::function<void(int)> cb)
{
    auto blk = &db->block_cache.at(path[path_pos]);
    if (blk->updating)
    {
        // Wait if block is being modified
        db->continue_update.emplace(blk->offset, [=]() { update_block(path_pos, is_delete, key, value, cb); });
        return;
    }
    if ((blk->type == KV_LEAF_SPLIT || blk->type == KV_INT_SPLIT) && prev_key_lt <= blk->right_half)
    {
        // This is a block split during previous updates
        // Its parent is already updated because prev_key_lt <= blk->right_half
        // That means we can safely remove the "split reference"
        blk->type = KV_LEAF;
        blk->key_lt = blk->right_half;
        blk->right_half = "";
        blk->right_half_block = 0;
    }
    uint32_t rm_size = 0;
    auto d_it = blk->data.find(key);
    if (d_it != blk->data.end())
    {
        if (!is_delete && d_it->second == value)
        {
            // Nothing to do
            cb(0);
            return;
        }
        rm_size = kv_block_t::kv_size(d_it->first, d_it->second);
    }
    else if (is_delete)
    {
        // Nothing to do
        cb(0);
        return;
    }
    if (cas_cb && path_pos == path.size()-1 && !cas_cb(d_it != blk->data.end() ? 0 : -ENOENT, d_it != blk->data.end() ? d_it->second : ""))
    {
        // CAS failure
        cb(-EAGAIN);
        return;
    }
    if (!is_delete)
    {
        blk->data[key] = value;
        blk->data_size = blk->data_size + kv_block_t::kv_size(key, value) - rm_size;
    }
    else
    {
        // FIXME We may want to merge blocks at some point, in that case we should:
        // - read both merged blocks
        // - add "merged into XX" reference to the second block and write it out
        // - add entries from the second block to the first one and write it out
        // - remove the reference from the parent block to the second block
        // - zero out the second block
        blk->data.erase(key);
        blk->data_size -= rm_size;
    }
    blk->updating = true;
    if (is_delete || blk->data_size < db->kv_block_size)
    {
        // No need to split the block => just modify and write it
        write_block(db, blk, [=](int res)
        {
            db->stop_updating(blk);
            if (res == -EINTR)
            {
                update();
            }
            else
            {
                cb(res);
            }
        });
        return;
    }
    // New item doesn't fit. The most interesting case.
    // Write the right half into a new block
    auto separator = find_splitter(db, blk);
    auto right_blk = create_new_block(db, blk, separator, true);
    write_new_block(db, right_blk, [=](int res)
    {
        if (res < 0)
        {
            db->stop_updating(blk);
            cb(res);
            return;
        }
        if (!blk->offset)
        {
            // Split the root block
            // Write the left half into a new block
            auto left_blk = create_new_block(db, blk, separator, false);
            write_new_block(db, left_blk, [=](int res)
            {
                if (res < 0)
                {
                    db->stop_updating(blk);
                    cb(res);
                    return;
                }
                // Write references to halves into the root block
                blk->type = KV_INT;
                db->base_block_level++;
                blk->level--;
                blk->data.clear();
                blk->data[""] = std::string((char*)&left_blk->offset, sizeof(left_blk->offset));
                blk->data[separator] = std::string((char*)&right_blk->offset, sizeof(right_blk->offset));
                blk->set_data_size();
                write_block(db, blk, [=](int write_res)
                {
                    db->stop_updating(blk);
                    if (write_res < 0)
                    {
                        del_block_level(db, blk);
                        db->block_cache.erase(blk->offset);
                        db->base_block_level--;
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
                        cb(0);
                    }
                });
            });
        }
        else
        {
            if (path_pos == 0)
            {
                // Block number zero should always be the root block
                cb(-EILSEQ);
                return;
            }
            // Split a non-root block
            blk->type = blk->type == KV_INT ? KV_INT_SPLIT : KV_LEAF_SPLIT;
            blk->right_half = separator;
            blk->right_half_block = right_blk->offset;
            blk->data.erase(blk->data.lower_bound(separator), blk->data.end());
            blk->set_data_size();
            write_block(db, blk, [=](int write_res)
            {
                db->stop_updating(blk);
                if (write_res < 0)
                {
                    del_block_level(db, blk);
                    db->block_cache.erase(blk->offset);
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
                    // Add a reference to the parent block
                    // Do not cleanup anything on failure because stored right_blk is already referenced
                    update_block(path_pos-1, false, separator, std::string((char*)&right_blk->offset, sizeof(right_blk->offset)), cb);
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
    if (path.size() == 0 || path[path.size()-1] != cur_block)
    {
        if (!cur_block)
            path.clear();
        path.push_back(cur_block);
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
        finish(cur_block == 0 ? -ENOENT : -EILSEQ);
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
    if (kv_it != blk->data.end() && kv_it->first == key && skip_equal)
    {
        kv_it++;
    }
    if (kv_it != blk->data.end())
    {
        // Send this item
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
        prev_key_ge = blk->right_half;
        prev_key_lt = blk->key_lt;
        cur_block = blk->right_half_block;
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
        key = blk->key_lt;
        skip_equal = false;
        path.pop_back();
        cur_level--;
        cur_block = path[path.size()-1];
        recheck_policy = KV_RECHECK_LEAF;
        // Check if we can resume listing from the next key
        auto pb_it = db->block_cache.find(cur_block);
        if (pb_it == db->block_cache.end())
        {
            // Block is absent in cache, recheck from the beginning
            prev_key_ge = prev_key_lt = "";
            cur_level = -db->base_block_level;
            cur_block = 0;
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

void kv_dbw_t::open(inode_t inode_id, uint32_t kv_block_size, std::function<void(int)> cb)
{
    db->open(inode_id, kv_block_size, cb);
}

uint64_t kv_dbw_t::get_size()
{
    return db->next_free;
}

void kv_dbw_t::close(std::function<void()> cb)
{
    db->close(cb);
}

void kv_dbw_t::get(const std::string & key, std::function<void(int res, const std::string & value)> cb)
{
    auto *op = new kv_op_t;
    op->db = db;
    op->opcode = KV_GET;
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
    op->opcode = KV_SET;
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
