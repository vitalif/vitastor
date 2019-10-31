#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <linux/fs.h>

#include <vector>
#include <map>

#include "allocator.h"
#include "sparsepp/sparsepp/spp.h"

// States are not stored on disk. Instead, they're deduced from the journal

#define ST_IN_FLIGHT 1
#define ST_J_WRITTEN 2
#define ST_J_SYNCED 3
#define ST_J_STABLE 4
#define ST_J_MOVED 5
#define ST_J_MOVE_SYNCED 6
#define ST_D_WRITTEN 16
#define ST_D_SYNCED 17
#define ST_D_META_WRITTEN 18
#define ST_D_META_SYNCED 19
#define ST_D_STABLE 20
#define ST_D_META_MOVED 21
#define ST_D_META_COMMITTED 22
#define ST_CURRENT 32
#define IS_STABLE(st) ((st) == 4 || (st) == 5 || (st) == 6 || (st) == 20 || (st) == 21 || (st) == 22 || (st) == 32)
#define IS_JOURNAL(st) (st >= 2 && st <= 6)

// Default object size is 128 KB
#define DEFAULT_ORDER 17
#define MAX_BLOCK_SIZE 128*1024*1024
#define DISK_ALIGNMENT 4096
#define MIN_JOURNAL_SIZE 4*1024*1024
#define JOURNAL_MAGIC 0x4A33

#define STRIPE_NUM(oid) ((oid) >> 4)
#define STRIPE_REPLICA(oid) ((oid) & 0xf)

// 16 bytes per object/stripe id
// stripe includes replica number in 4 least significant bits
struct __attribute__((__packed__)) object_id
{
    uint64_t inode;
    uint64_t stripe;
};

bool operator == (const object_id & a, const object_id & b)
{
    return b.inode == a.inode && b.stripe == a.stripe;
}

// 32 bytes per "clean" entry on disk with fixed metadata tables
struct __attribute__((__packed__)) clean_disk_entry
{
    uint64_t inode;
    uint64_t stripe;
    uint64_t version;
    uint8_t flags;
    uint8_t reserved[7];
};

// 28 bytes per "clean" entry in memory
struct __attribute__((__packed__)) clean_entry
{
    uint64_t version;
    uint32_t state;
    uint64_t location;
};

// 48 bytes per dirty entry in memory
struct __attribute__((__packed__)) dirty_entry
{
    uint64_t version;
    uint32_t state;
    uint32_t flags;
    uint64_t location; // location in either journal or data
    uint32_t offset;   // offset within stripe
    uint32_t size;     // entry size
};

// Journal entries
// Journal entries are linked to each other by their crc32 value
// The journal is almost a blockchain, because object versions constantly increase
#define JE_START       0x01
#define JE_SMALL_WRITE 0x02
#define JE_BIG_WRITE   0x03
#define JE_STABLE      0x04
#define JE_DELETE      0x05

struct __attribute__((__packed__)) journal_entry_start
{
    uint32_t type;
    uint32_t size;
    uint32_t crc32;
    uint32_t reserved1;
    uint64_t offset;
};

struct __attribute__((__packed__)) journal_entry_small_write
{
    uint32_t type;
    uint32_t size;
    uint32_t crc32;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
    uint32_t offset;
    uint32_t len;
};

struct __attribute__((__packed__)) journal_entry_big_write
{
    uint32_t type;
    uint32_t size;
    uint32_t crc32;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
    uint64_t block;
};

struct __attribute__((__packed__)) journal_entry_stable
{
    uint32_t type;
    uint32_t size;
    uint32_t crc32;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
};

struct __attribute__((__packed__)) journal_entry_del
{
    uint32_t type;
    uint32_t size;
    uint32_t crc32;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
};

struct __attribute__((__packed__)) journal_entry
{
    union
    {
        struct __attribute__((__packed__))
        {
            uint16_t magic;
            uint16_t type;
            uint32_t size;
            uint32_t crc32;
        };
        journal_entry_start start;
        journal_entry_small_write small_write;
        journal_entry_big_write big_write;
        journal_entry_stable stable;
        journal_entry_del del;
    };
};

typedef std::vector<dirty_entry> dirty_list;

class oid_hash
{
public:
    size_t operator()(const object_id &s) const
    {
        size_t seed = 0;
        spp::hash_combine(seed, s.inode);
        spp::hash_combine(seed, s.stripe);
        return seed;
    }
};

class blockstore
{
public:
    spp::sparse_hash_map<object_id, clean_entry, oid_hash> object_db;
    spp::sparse_hash_map<object_id, dirty_list, oid_hash> dirty_queue;
    int block_order, block_size;
    uint64_t block_count;
    allocator *data_alloc;

    int journal_fd;
    int meta_fd;
    int data_fd;

    uint64_t journal_offset, journal_size, journal_len;
    uint64_t meta_offset, meta_size, meta_len;
    uint64_t data_offset, data_size, data_len;

    uint64_t journal_start, journal_end;

    blockstore(spp::sparse_hash_map<std::string, std::string> & config)
    {
        block_order = stoll(config["block_size_order"]);
        block_size = 1 << block_order;
        if (block_size <= 1 || block_size >= MAX_BLOCK_SIZE)
        {
            throw new std::runtime_error("Bad block size");
        }
        data_fd = meta_fd = journal_fd = -1;
        try
        {
            open_data(config);
            open_meta(config);
            open_journal(config);
            calc_lengths(config);
            data_alloc = allocator_create(block_count);
            if (!data_alloc)
                throw new std::bad_alloc();
        }
        catch (std::exception & e)
        {
            if (data_fd >= 0)
                close(data_fd);
            if (meta_fd >= 0 && meta_fd != data_fd)
                close(meta_fd);
            if (journal_fd >= 0 && journal_fd != meta_fd)
                close(journal_fd);
            throw e;
        }
    }

    ~blockstore()
    {
        if (data_fd >= 0)
            close(data_fd);
        if (meta_fd >= 0 && meta_fd != data_fd)
            close(meta_fd);
        if (journal_fd >= 0 && journal_fd != meta_fd)
            close(journal_fd);
    }

    void calc_lengths(spp::sparse_hash_map<std::string, std::string> & config)
    {
        // data
        data_len = data_size - data_offset;
        if (data_fd == meta_fd && data_offset < meta_offset)
        {
            data_len = meta_offset - data_offset;
        }
        if (data_fd == journal_fd && data_offset < journal_offset)
        {
            data_len = data_len < journal_offset-data_offset
                ? data_len : journal_offset-data_offset;
        }
        // meta
        meta_len = (meta_fd == data_fd ? data_size : meta_size) - meta_offset;
        if (meta_fd == data_fd && meta_offset < data_offset)
        {
            meta_len = data_offset - meta_offset;
        }
        if (meta_fd == journal_fd && meta_offset < journal_offset)
        {
            meta_len = meta_len < journal_offset-meta_offset
                ? meta_len : journal_offset-meta_offset;
        }
        // journal
        journal_len = (journal_fd == data_fd ? data_size : (journal_fd == meta_fd ? meta_size : journal_size)) - journal_offset;
        if (journal_fd == data_fd && journal_offset < data_offset)
        {
            journal_len = data_offset - journal_offset;
        }
        if (journal_fd == meta_fd && journal_offset < meta_offset)
        {
            journal_len = journal_len < meta_offset-journal_offset
                ? journal_len : meta_offset-journal_offset;
        }
        // required metadata size
        block_count = data_len / block_size;
        uint64_t meta_required = block_count * sizeof(clean_disk_entry);
        if (meta_len < meta_required)
        {
            throw new std::runtime_error("Metadata area is too small");
        }
        // requested journal size
        uint64_t journal_wanted = stoll(config["journal_size"]);
        if (journal_wanted > journal_len)
        {
            throw new std::runtime_error("Requested journal_size is too large");
        }
        else if (journal_wanted > 0)
        {
            journal_len = journal_wanted;
        }
        if (journal_len < MIN_JOURNAL_SIZE)
        {
            throw new std::runtime_error("Journal is too small");
        }
    }

    void open_data(spp::sparse_hash_map<std::string, std::string> & config)
    {
        int sectsize;
        data_offset = stoll(config["data_offset"]);
        if (data_offset % DISK_ALIGNMENT)
        {
            throw new std::runtime_error("data_offset not aligned");
        }
        data_fd = open(config["data_device"].c_str(), O_DIRECT|O_RDWR);
        if (data_fd == -1)
        {
            throw new std::runtime_error("Failed to open data device");
        }
        if (ioctl(data_fd, BLKSSZGET, &sectsize) < 0 ||
            ioctl(data_fd, BLKGETSIZE64, &data_size) < 0 ||
            sectsize != 512)
        {
            throw new std::runtime_error("Data device sector is not equal to 512 bytes");
        }
        if (data_offset >= data_size)
        {
            throw new std::runtime_error("data_offset exceeds device size");
        }
    }

    void open_meta(spp::sparse_hash_map<std::string, std::string> & config)
    {
        int sectsize;
        meta_offset = stoll(config["meta_offset"]);
        if (meta_offset % DISK_ALIGNMENT)
        {
            throw new std::runtime_error("meta_offset not aligned");
        }
        if (config["meta_device"] != "")
        {
            meta_offset = 0;
            meta_fd = open(config["meta_device"].c_str(), O_DIRECT|O_RDWR);
            if (meta_fd == -1)
            {
                throw new std::runtime_error("Failed to open metadata device");
            }
            if (ioctl(meta_fd, BLKSSZGET, &sectsize) < 0 ||
                ioctl(meta_fd, BLKGETSIZE64, &meta_size) < 0 ||
                sectsize != 512)
            {
                throw new std::runtime_error("Metadata device sector is not equal to 512 bytes (or ioctl failed)");
            }
            if (meta_offset >= meta_size)
            {
                throw new std::runtime_error("meta_offset exceeds device size");
            }
        }
        else
        {
            meta_fd = data_fd;
            meta_size = 0;
            if (meta_offset >= data_size)
            {
                throw new std::runtime_error("meta_offset exceeds device size");
            }
        }
    }

    void open_journal(spp::sparse_hash_map<std::string, std::string> & config)
    {
        int sectsize;
        journal_offset = stoll(config["journal_offset"]);
        if (journal_offset % DISK_ALIGNMENT)
        {
            throw new std::runtime_error("journal_offset not aligned");
        }
        if (config["journal_device"] != "")
        {
            journal_fd = open(config["journal_device"].c_str(), O_DIRECT|O_RDWR);
            if (journal_fd == -1)
            {
                throw new std::runtime_error("Failed to open journal device");
            }
            if (ioctl(journal_fd, BLKSSZGET, &sectsize) < 0 ||
                ioctl(journal_fd, BLKGETSIZE64, &journal_size) < 0 ||
                sectsize != 512)
            {
                throw new std::runtime_error("Journal device sector is not equal to 512 bytes");
            }
        }
        else
        {
            journal_fd = meta_fd;
            journal_size = 0;
            if (journal_offset >= data_size)
            {
                throw new std::runtime_error("journal_offset exceeds device size");
            }
        }
    }

    struct read_fulfill
    {
        uint64_t flags;
        uint64_t offset;
        uint64_t len;
        void *buf;
    };

    void fulfill_read(std::map<uint64_t, read_fulfill> & fulfill, uint8_t* buf, uint32_t offset, uint32_t len,
        uint32_t item_start, uint32_t dirty_end, uint32_t item_state, uint64_t item_location)
    {
        uint32_t dirty_start = item_start;
        if (dirty_start < offset+len && dirty_end > offset)
        {
            dirty_start = dirty_start < offset ? offset : dirty_start;
            dirty_end = dirty_end > offset+len ? offset+len : dirty_end;
            auto fulfill_near = fulfill.lower_bound(dirty_start);
            if (fulfill_near != fulfill.begin())
            {
                fulfill_near--;
                if (fulfill_near->second.offset + fulfill_near->second.len <= dirty_start)
                    fulfill_near++;
            }
            while (fulfill_near != fulfill.end() && fulfill_near->second.offset < dirty_end)
            {
                if (fulfill_near->second.offset > dirty_start)
                {
                    fulfill[dirty_start] = (read_fulfill){
                        item_state,
                        item_location + dirty_start - item_start,
                        fulfill_near->second.offset - dirty_start,
                        buf + dirty_start - offset,
                    };
                }
                dirty_start = fulfill_near->second.offset + fulfill_near->second.len;
            }
            if (dirty_start < dirty_end)
            {
                fulfill[dirty_start] = (read_fulfill){
                    item_state,
                    item_location + dirty_start - item_start,
                    dirty_end - dirty_start,
                    buf + dirty_start - offset
                };
            }
        }
    }

    // flags: READ_DIRTY
#define READ_DIRTY 1
    int read(object_id oid, uint32_t offset, uint32_t len, uint32_t flags, uint8_t *buf, void (*callback)(int arg), int arg)
    {
        auto clean_it = object_db.find(oid);
        auto dirty_it = dirty_queue.find(oid);
        if (clean_it == object_db.end() && dirty_it == object_db.end())
        {
            memset(buf, 0, len);
            callback(arg);
            return 0;
        }
        uint64_t fulfilled = 0;
        std::map<uint64_t, read_fulfill> fulfill;
        //std::vector<read_fulfill> fulfill;
        if (dirty_it != object_db.end())
        {
            dirty_list dirty = dirty_it->second;
            for (int i = dirty.size()-1; i >= 0; i--)
            {
                if ((flags & READ_DIRTY) || IS_STABLE(dirty[i].state))
                {
                    fulfill_read(fulfill, buf, offset, len, dirty[i].offset, dirty[i].offset + dirty[i].size, IS_JOURNAL(dirty[i].state), dirty[i].location);
                }
            }
        }
        if (clean_it != object_db.end())
        {
            fulfill_read(fulfill, buf, offset, len, 0, block_size, 0, clean_it->second.location);
        }
    }
};
