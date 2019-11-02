#include "blockstore.h"

void blockstore::calc_lengths(spp::sparse_hash_map<std::string, std::string> & config)
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
    meta_area = (meta_fd == data_fd ? data_size : meta_size) - meta_offset;
    if (meta_fd == data_fd && meta_offset < data_offset)
    {
        meta_area = data_offset - meta_offset;
    }
    if (meta_fd == journal_fd && meta_offset < journal_offset)
    {
        meta_area = meta_area < journal_offset-meta_offset
            ? meta_area : journal_offset-meta_offset;
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
    meta_len = block_count * sizeof(clean_disk_entry);
    if (meta_area < meta_len)
    {
        throw new std::runtime_error("Metadata area is too small");
    }
    metadata_buf_size = stoull(config["meta_buf_size"]);
    if (metadata_buf_size < 65536)
    {
        metadata_buf_size = 4*1024*1024;
    }
    // requested journal size
    uint64_t journal_wanted = stoull(config["journal_size"]);
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

void blockstore::open_data(spp::sparse_hash_map<std::string, std::string> & config)
{
    int sectsize;
    data_offset = stoull(config["data_offset"]);
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

void blockstore::open_meta(spp::sparse_hash_map<std::string, std::string> & config)
{
    int sectsize;
    meta_offset = stoull(config["meta_offset"]);
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

void blockstore::open_journal(spp::sparse_hash_map<std::string, std::string> & config)
{
    int sectsize;
    journal_offset = stoull(config["journal_offset"]);
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
