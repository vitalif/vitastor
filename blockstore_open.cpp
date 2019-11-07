#include "blockstore.h"

void blockstore::calc_lengths(spp::sparse_hash_map<std::string, std::string> & config)
{
    // data
    data_len = data_size - data_offset;
    if (data_fd == meta_fd && data_offset < meta_offset)
    {
        data_len = meta_offset - data_offset;
    }
    if (data_fd == journal.fd && data_offset < journal.offset)
    {
        data_len = data_len < journal.offset-data_offset
            ? data_len : journal.offset-data_offset;
    }
    // meta
    meta_area = (meta_fd == data_fd ? data_size : meta_size) - meta_offset;
    if (meta_fd == data_fd && meta_offset < data_offset)
    {
        meta_area = data_offset - meta_offset;
    }
    if (meta_fd == journal.fd && meta_offset < journal.offset)
    {
        meta_area = meta_area < journal.offset-meta_offset
            ? meta_area : journal.offset-meta_offset;
    }
    // journal
    journal.len = (journal.fd == data_fd ? data_size : (journal.fd == meta_fd ? meta_size : journal.device_size)) - journal.offset;
    if (journal.fd == data_fd && journal.offset < data_offset)
    {
        journal.len = data_offset - journal.offset;
    }
    if (journal.fd == meta_fd && journal.offset < meta_offset)
    {
        journal.len = journal.len < meta_offset-journal.offset
            ? journal.len : meta_offset-journal.offset;
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
    if (journal_wanted > journal.len)
    {
        throw new std::runtime_error("Requested journal_size is too large");
    }
    else if (journal_wanted > 0)
    {
        journal.len = journal_wanted;
    }
    if (journal.len < MIN_JOURNAL_SIZE)
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
    journal.offset = stoull(config["journal_offset"]);
    if (journal.offset % DISK_ALIGNMENT)
    {
        throw new std::runtime_error("journal_offset not aligned");
    }
    if (config["journal_device"] != "")
    {
        journal.fd = open(config["journal_device"].c_str(), O_DIRECT|O_RDWR);
        if (journal.fd == -1)
        {
            throw new std::runtime_error("Failed to open journal device");
        }
        if (ioctl(journal.fd, BLKSSZGET, &sectsize) < 0 ||
            ioctl(journal.fd, BLKGETSIZE64, &journal.device_size) < 0 ||
            sectsize != 512)
        {
            throw new std::runtime_error("Journal device sector is not equal to 512 bytes");
        }
    }
    else
    {
        journal.fd = meta_fd;
        journal.device_size = 0;
        if (journal.offset >= data_size)
        {
            throw new std::runtime_error("journal_offset exceeds device size");
        }
    }
    journal.sector_count = stoull(config["journal_sector_buffer_count"]);
    if (!journal.sector_count)
    {
        journal.sector_count = 32;
    }
    journal.sector_buf = (uint8_t*)memalign(512, journal.sector_count * 512);
    journal.sector_info = (journal_sector_info_t*)calloc(journal.sector_count, sizeof(journal_sector_info_t));
    if (!journal.sector_buf || !journal.sector_info)
    {
        throw new std::bad_alloc();
    }
}
