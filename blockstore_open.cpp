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
    meta_len = (block_count / (512 / sizeof(clean_disk_entry))) * 512;
    if (meta_area < meta_len)
    {
        throw std::runtime_error("Metadata area is too small");
    }
    metadata_buf_size = strtoull(config["meta_buf_size"].c_str(), NULL, 10);
    if (metadata_buf_size < 65536)
    {
        metadata_buf_size = 4*1024*1024;
    }
    // requested journal size
    uint64_t journal_wanted = strtoull(config["journal_size"].c_str(), NULL, 10);
    if (journal_wanted > journal.len)
    {
        throw std::runtime_error("Requested journal_size is too large");
    }
    else if (journal_wanted > 0)
    {
        journal.len = journal_wanted;
    }
    if (journal.len < MIN_JOURNAL_SIZE)
    {
        throw std::runtime_error("Journal is too small");
    }
}

void check_size(int fd, uint64_t *size, std::string name)
{
    int sectsize;
    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        throw std::runtime_error("Failed to stat "+name);
    }
    if (S_ISREG(st.st_mode))
    {
        *size = st.st_size;
    }
    else if (S_ISBLK(st.st_mode))
    {
        if (ioctl(fd, BLKSSZGET, &sectsize) < 0 ||
            ioctl(fd, BLKGETSIZE64, size) < 0 ||
            sectsize != 512)
        {
            throw std::runtime_error(name+" sector is not equal to 512 bytes");
        }
    }
    else
    {
        throw std::runtime_error(name+" is neither a file nor a block device");
    }
}

void blockstore::open_data(spp::sparse_hash_map<std::string, std::string> & config)
{
    data_offset = strtoull(config["data_offset"].c_str(), NULL, 10);
    if (data_offset % DISK_ALIGNMENT)
    {
        throw std::runtime_error("data_offset not aligned");
    }
    data_fd = open(config["data_device"].c_str(), O_DIRECT|O_RDWR);
    if (data_fd == -1)
    {
        throw std::runtime_error("Failed to open data device");
    }
    check_size(data_fd, &data_size, "data device");
    if (data_offset >= data_size)
    {
        throw std::runtime_error("data_offset exceeds device size");
    }
}

void blockstore::open_meta(spp::sparse_hash_map<std::string, std::string> & config)
{
    meta_offset = strtoull(config["meta_offset"].c_str(), NULL, 10);
    if (meta_offset % DISK_ALIGNMENT)
    {
        throw std::runtime_error("meta_offset not aligned");
    }
    if (config["meta_device"] != "")
    {
        meta_offset = 0;
        meta_fd = open(config["meta_device"].c_str(), O_DIRECT|O_RDWR);
        if (meta_fd == -1)
        {
            throw std::runtime_error("Failed to open metadata device");
        }
        check_size(meta_fd, &meta_size, "metadata device");
        if (meta_offset >= meta_size)
        {
            throw std::runtime_error("meta_offset exceeds device size");
        }
    }
    else
    {
        meta_fd = data_fd;
        meta_size = 0;
        if (meta_offset >= data_size)
        {
            throw std::runtime_error("meta_offset exceeds device size");
        }
    }
}

void blockstore::open_journal(spp::sparse_hash_map<std::string, std::string> & config)
{
    journal.offset = strtoull(config["journal_offset"].c_str(), NULL, 10);
    if (journal.offset % DISK_ALIGNMENT)
    {
        throw std::runtime_error("journal_offset not aligned");
    }
    if (config["journal_device"] != "")
    {
        journal.fd = open(config["journal_device"].c_str(), O_DIRECT|O_RDWR);
        if (journal.fd == -1)
        {
            throw std::runtime_error("Failed to open journal device");
        }
        check_size(journal.fd, &journal.device_size, "metadata device");
    }
    else
    {
        journal.fd = meta_fd;
        journal.device_size = 0;
        if (journal.offset >= data_size)
        {
            throw std::runtime_error("journal_offset exceeds device size");
        }
    }
    journal.sector_count = strtoull(config["journal_sector_buffer_count"].c_str(), NULL, 10);
    if (!journal.sector_count)
    {
        journal.sector_count = 32;
    }
    journal.sector_buf = (uint8_t*)memalign(512, journal.sector_count * 512);
    journal.sector_info = (journal_sector_info_t*)calloc(journal.sector_count, sizeof(journal_sector_info_t));
    if (!journal.sector_buf || !journal.sector_info)
    {
        throw std::bad_alloc();
    }
}
