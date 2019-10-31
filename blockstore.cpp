#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
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

#define DEFAULT_ORDER 17
#define MAX_BLOCK_SIZE 128*1024*1024
#define DISK_ALIGNMENT 4096
#define MIN_JOURNAL_SIZE 4*1024*1024

#define STRIPE_NUM(oid) ((oid) >> 4)
#define STRIPE_REPLICA(oid) ((oid) & 0xf)

struct __attribute__((__packed__)) oid
{
    uint64_t inode;
    uint64_t stripe;
};

struct __attribute__((__packed__)) meta_entry
{
    uint64_t inode;
    uint64_t stripe;
    uint32_t epoch;
    uint32_t version;
    uint64_t location_flags;
};

struct __attribute__((__packed__)) object_version
{
    uint32_t epoch;
    uint32_t version;
    uint64_t location;
    uint32_t size;
    uint32_t state;

    bool in_journal()
    {
        return (location & (1 << 63));
    }

    uint64_t offset()
    {
        return (location & ~(1 << 63));
    }
};

struct __attribute__((__packed__)) object_ver_list
{
    uint64_t count;
    object_version versions[];
};

struct __attribute__((__packed__)) object_info
{
    object_version first;
    object_ver_list *other;
};

class oid_hash
{
public:
    size_t operator()(const oid &s) const
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
    spp::sparse_hash_map<oid, object_info, oid_hash> object_db;
    int block_order, block_size;
    uint64_t block_count;
    allocator *data_alloc;
    int journal_fd;
    int meta_fd;
    int data_fd;
    uint64_t journal_offset, journal_size, journal_len;
    uint64_t meta_offset, meta_size, meta_len;
    uint64_t data_offset, data_size, data_len;

    blockstore(std::unordered_map<std::string, std::string> & config)
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

    void calc_lengths(std::unordered_map<std::string, std::string> & config)
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
        uint64_t meta_required = block_count * sizeof(meta_entry);
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

    void open_data(std::unordered_map<std::string, std::string> & config)
    {
        int sectsize;
        data_offset = stoll(config["data_offset"]);
        if (data_offset % DISK_ALIGNMENT)
        {
            throw new std::runtime_error("data_offset not aligned");
        }
        data_fd = open(config["data_device"], O_DIRECT|O_RDWR);
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

    void open_meta(std::unordered_map<std::string, std::string> & config)
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
            meta_fd = open(config["meta_device"], O_DIRECT|O_RDWR);
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

    void open_journal(std::unordered_map<std::string, std::string> & config)
    {
        int sectsize;
        journal_offset = stoll(config["journal_offset"]);
        if (journal_offset % DISK_ALIGNMENT)
        {
            throw new std::runtime_error("journal_offset not aligned");
        }
        if (config["journal_device"] != "")
        {
            journal_fd = open(config["journal_device"], O_DIRECT|O_RDWR);
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

    int read(oid stripe, uint32_t offset, uint32_t len, void *buf, void (*callback)(int arg), int arg)
    {
        auto o = object_db.find(stripe);
        if (o == object_db.end())
        {
            memset(buf, 0, len);
            callback(arg);
            return;
        }
        auto info = o->second;
    }
};
