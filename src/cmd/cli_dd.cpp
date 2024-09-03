// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <algorithm>

// Copy data between Vitastor images, files and pipes
// A showpiece implementation of dd :-) with iodepth, asynchrony, pipe support and so on

struct dd_buf_t
{
    void *buf = NULL;
    uint64_t offset = 0, len = 0, max = 0;

    dd_buf_t(uint64_t offset, uint64_t max)
    {
        this->offset = offset;
        this->max = max;
        this->buf = malloc_or_die(max);
    }

    ~dd_buf_t()
    {
        free(this->buf);
        this->buf = NULL;
    }
};

struct dd_in_info_t
{
    // in
    std::string iimg, ifile;
    bool in_direct = false;
    bool detect_size = true;

    // out
    cli_result_t result;
    inode_watch_t *iwatch = NULL;
    int ifd = -1;
    uint64_t in_size = 0;
    uint32_t in_granularity = 1;
    bool in_seekable = false;

    void open_input(cli_tool_t *parent)
    {
        in_seekable = true;
        if (iimg != "")
        {
            iwatch = parent->cli->st_cli.watch_inode(iimg);
            if (!iwatch->cfg.num)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Image "+iimg+" does not exist" };
                parent->cli->st_cli.close_watch(iwatch);
                iwatch = NULL;
                return;
            }
            auto pool_it = parent->cli->st_cli.pool_config.find(INODE_POOL(iwatch->cfg.num));
            if (pool_it == parent->cli->st_cli.pool_config.end())
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool of image "+iimg+" does not exist" };
                parent->cli->st_cli.close_watch(iwatch);
                iwatch = NULL;
                return;
            }
            in_granularity = pool_it->second.bitmap_granularity;
            if (detect_size)
            {
                in_size = iwatch->cfg.size;
            }
        }
        else if (ifile != "")
        {
            ifd = open(ifile.c_str(), (in_direct ? O_DIRECT : 0) | O_RDONLY);
            if (ifd < 0)
            {
                result = (cli_result_t){ .err = errno, .text = "Failed to open "+ifile+": "+std::string(strerror(errno)) };
                return;
            }
            if (detect_size)
            {
                struct stat st;
                if (fstat(ifd, &st) < 0)
                {
                    result = (cli_result_t){ .err = errno, .text = "Failed to stat "+ifile+": "+std::string(strerror(errno)) };
                    close(ifd);
                    ifd = -1;
                    return;
                }
                if (S_ISREG(st.st_mode))
                {
                    in_size = st.st_size;
                }
                else if (S_ISBLK(st.st_mode))
                {
                    if (ioctl(ifd, BLKGETSIZE64, &in_size) < 0)
                    {
                        result = (cli_result_t){ .err = errno, .text = "Failed to get "+ifile+" size: "+std::string(strerror(errno)) };
                        close(ifd);
                        ifd = -1;
                        return;
                    }
                }
            }
            if (in_direct)
            {
                in_granularity = 512;
            }
            if (lseek(ifd, 1, SEEK_SET) == (off_t)-1)
            {
                in_seekable = false;
            }
            else
            {
                lseek(ifd, 0, SEEK_SET);
            }
        }
        else
        {
            ifd = 0;
            in_seekable = false;
        }
    }

    void close_input(cli_tool_t *parent)
    {
        if (iimg != "")
        {
            parent->cli->st_cli.close_watch(iwatch);
            iwatch = NULL;
        }
        else if (ifile != "")
        {
            close(ifd);
            ifd = -1;
        }
    }
};

struct dd_out_info_t
{
    std::string oimg, ofile;
    std::string out_pool;
    bool out_direct = false;
    bool out_create = true;
    bool out_trunc = false;
    bool out_append = false;
    bool end_fsync = true;
    uint64_t out_size = 0;

    cli_result_t result;
    bool old_progress = false;
    inode_watch_t *owatch = NULL;
    int ofd = -1;
    uint32_t out_granularity = 1;
    bool out_seekable = false;
    std::function<bool(cli_result_t &)> sub_cb;

    pool_config_t *find_pool(cli_tool_t *parent, const std::string & name)
    {
        if (name == "" && parent->cli->st_cli.pool_config.size() == 1)
        {
            return &parent->cli->st_cli.pool_config.begin()->second;
        }
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            if (pp.second.name == name)
            {
                return &pp.second;
            }
        }
        return NULL;
    }

    bool open_output(cli_tool_t *parent, int & state, int base_state)
    {
        if (state == base_state)
            goto resume_1;
        else if (state == base_state+1)
            goto resume_2;
        if (oimg != "")
        {
            out_seekable = true;
            owatch = parent->cli->st_cli.watch_inode(oimg);
            if (owatch->cfg.num)
            {
                auto pool_it = parent->cli->st_cli.pool_config.find(INODE_POOL(owatch->cfg.num));
                if (pool_it == parent->cli->st_cli.pool_config.end())
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Pool of image "+oimg+" does not exist" };
                    parent->cli->st_cli.close_watch(owatch);
                    owatch = NULL;
                    return true;
                }
                out_granularity = pool_it->second.bitmap_granularity;
            }
            else
            {
                auto pool_cfg = find_pool(parent, out_pool);
                if (pool_cfg)
                {
                    out_granularity = pool_cfg->bitmap_granularity;
                }
                else
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Pool to create output image "+oimg+" is not specified" };
                    parent->cli->st_cli.close_watch(owatch);
                    owatch = NULL;
                    return true;
                }
            }
            if (out_size % 4096)
            {
                out_size += (4096 - (out_size % 4096));
            }
            old_progress = parent->progress;
            if (!owatch->cfg.num)
            {
                if (!out_create)
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Image "+oimg+" does not exist" };
                    parent->cli->st_cli.close_watch(owatch);
                    owatch = NULL;
                    return true;
                }
                if (!out_size)
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Input size is unknown, specify size to create output image "+oimg };
                    parent->cli->st_cli.close_watch(owatch);
                    owatch = NULL;
                    return true;
                }
                // Create output image
                sub_cb = parent->start_create(json11::Json::object {
                    { "image", oimg },
                    { "pool", out_pool },
                    { "size", out_size },
                });
            }
            else if (owatch->cfg.size < out_size || out_trunc)
            {
                if (!out_size)
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Input size is unknown, specify size to truncate output image" };
                    parent->cli->st_cli.close_watch(owatch);
                    owatch = NULL;
                    return true;
                }
                // Resize output image
                parent->progress = false;
                sub_cb = parent->start_modify(json11::Json::object {
                    { "image", oimg },
                    { "resize", out_size },
                });
            }
            else
            {
                // ok
                return true;
            }
            // Wait for sub-command
resume_1:
            while (!sub_cb(result))
            {
                state = base_state;
                return false;
            }
            parent->progress = old_progress;
            sub_cb = NULL;
            if (result.err)
            {
                parent->cli->st_cli.close_watch(owatch);
                owatch = NULL;
                return true;
            }
            // Wait until output image actually appears
resume_2:
            while (!owatch->cfg.num)
            {
                state = base_state+1;
                return false;
            }
        }
        else if (ofile != "")
        {
            ofd = open(ofile.c_str(), (out_direct ? O_DIRECT : 0) | (out_append ? O_APPEND : O_RDWR) | (out_create ? O_CREAT : 0), 0666);
            if (ofd < 0)
            {
                result = (cli_result_t){ .err = errno, .text = "Failed to open "+ofile+": "+std::string(strerror(errno)) };
                return true;
            }
            if (out_trunc && ftruncate(ofd, out_size) < 0)
            {
                result = (cli_result_t){ .err = errno, .text = "Failed to truncate "+ofile+": "+std::string(strerror(errno)) };
                return true;
            }
            if (out_direct)
            {
                out_granularity = 512;
            }
            out_seekable = !out_append;
        }
        else
        {
            ofd = 1;
            out_seekable = false;
        }
        return true;
    }

    bool fsync_output(cli_tool_t *parent, int & state, int base_state)
    {
        if (state == base_state)
            goto resume_1;
        if (oimg != "")
        {
            {
                cluster_op_t *sync_op = new cluster_op_t;
                sync_op->opcode = OSD_OP_SYNC;
                parent->waiting++;
                sync_op->callback = [this, parent](cluster_op_t *sync_op)
                {
                    parent->waiting--;
                    delete sync_op;
                    parent->ringloop->wakeup();
                };
                parent->cli->execute(sync_op);
            }
        resume_1:
            if (parent->waiting > 0)
            {
                state = base_state;
                return false;
            }
        }
        else
        {
            int res = fsync(ofd);
            if (res < 0)
            {
                result = (cli_result_t){ .err = errno, .text = "Failed to fsync "+ofile+": "+std::string(strerror(errno)) };
            }
        }
        return true;
    }

    void close_output(cli_tool_t *parent)
    {
        if (oimg != "")
        {
            parent->cli->st_cli.close_watch(owatch);
            owatch = NULL;
        }
        else
        {
            if (ofile != "")
                close(ofd);
            ofd = -1;
        }
    }
};

struct cli_dd_t
{
    cli_tool_t *parent;

    dd_in_info_t iinfo;
    dd_out_info_t oinfo;

    uint64_t blocksize = 0, bytelimit = 0, iseek = 0, oseek = 0, iodepth = 0;
    bool end_status = true, ignore_errors = false;
    bool write_zero = false;

    uint64_t in_iodepth = 0, out_iodepth = 0;
    uint64_t read_offset = 0, read_end = 0;
    std::vector<dd_buf_t*> read_buffers, short_reads, short_writes;
    std::vector<uint8_t*> zero_buf;
    bool in_eof = false;
    uint64_t written_size = 0;
    uint64_t written_progress = 0;
    timespec tv_begin = {}, tv_progress = {};
    int state = 0;
    int copy_error = 0;
    int in_waiting = 0, out_waiting = 0;
    cli_result_t result;

    bool is_done()
    {
        return state == 100;
    }

    int skip_read(int fd, uint64_t to_skip)
    {
        void *buf = malloc_or_die(blocksize);
        while (to_skip > 0)
        {
            auto res = read(fd, buf, blocksize < to_skip ? blocksize : to_skip);
            if (res <= 0)
            {
                return res == 0 ? -EPIPE : -errno;
            }
            to_skip -= res;
        }
        free(buf);
        return 0;
    }

    uint64_t round_up(uint64_t n, uint64_t align)
    {
        return (n % align) ? (n + align - (n % align)) : n;
    }

    void vitastor_read_bitmap(dd_buf_t *cur_read)
    {
        cluster_op_t *read_op = new cluster_op_t;
        read_op->opcode = OSD_OP_READ_CHAIN_BITMAP;
        read_op->inode = iinfo.iwatch->cfg.num;
        // FIXME: Support unaligned read?
        read_op->offset = cur_read->offset + iseek;
        read_op->len = round_up(round_up(cur_read->max, iinfo.in_granularity), oinfo.out_granularity);
        in_waiting++;
        read_op->callback = [this, cur_read](cluster_op_t *read_op)
        {
            in_waiting--;
            if (read_op->retval < 0)
            {
                fprintf(
                    stderr, "Failed to read bitmap for %lu bytes from image %s at offset %lu: %s (code %d)\n",
                    read_op->len, iinfo.iimg.c_str(), read_op->offset,
                    strerror(read_op->retval < 0 ? -read_op->retval : EIO), read_op->retval
                );
                if (!ignore_errors)
                {
                    copy_error = read_op->retval < 0 ? -read_op->retval : EIO;
                }
                delete cur_read;
            }
            else if (!is_zero(read_op->bitmap_buf, read_op->len/iinfo.in_granularity/8))
            {
                vitastor_read(cur_read);
            }
            else
            {
                delete cur_read;
            }
            delete read_op;
            parent->ringloop->wakeup();
        };
        parent->cli->execute(read_op);
    }

    void vitastor_read(dd_buf_t *cur_read)
    {
        cluster_op_t *read_op = new cluster_op_t;
        read_op->opcode = OSD_OP_READ;
        read_op->inode = iinfo.iwatch->cfg.num;
        // FIXME: Support unaligned read?
        read_op->offset = cur_read->offset + iseek;
        read_op->len = round_up(round_up(cur_read->max, iinfo.in_granularity), oinfo.out_granularity);
        read_op->iov.push_back(cur_read->buf, cur_read->max);
        if (cur_read->max < read_op->len)
        {
            // Zero pad
            read_op->iov.push_back(zero_buf.data(), read_op->len - cur_read->max);
        }
        in_waiting++;
        read_op->callback = [this, cur_read](cluster_op_t *read_op)
        {
            in_waiting--;
            if (read_op->retval != read_op->len)
            {
                fprintf(
                    stderr, "Failed to read %lu bytes from image %s at offset %lu: %s (code %d)\n",
                    read_op->len, iinfo.iimg.c_str(), read_op->offset,
                    strerror(read_op->retval < 0 ? -read_op->retval : EIO), read_op->retval
                );
                if (!ignore_errors)
                {
                    copy_error = read_op->retval < 0 ? -read_op->retval : EIO;
                }
                delete cur_read;
            }
            else
            {
                cur_read->len = cur_read->max;
                add_finished_read(cur_read);
            }
            delete read_op;
            parent->ringloop->wakeup();
        };
        parent->cli->execute(read_op);
    }

    bool add_read_op()
    {
        if (iinfo.iwatch)
        {
            dd_buf_t *cur_read = new dd_buf_t(read_offset, read_offset + blocksize > read_end ? read_end - read_offset : blocksize);
            read_offset += cur_read->max;
            in_eof = read_offset >= read_end;
            cur_read->len = cur_read->max;
            if (!write_zero)
            {
                vitastor_read_bitmap(cur_read);
            }
            else
            {
                vitastor_read(cur_read);
            }
        }
        else
        {
            io_uring_sqe *sqe = parent->ringloop->get_sqe();
            if (!sqe)
            {
                return false;
            }
            dd_buf_t *cur_read;
            if (short_reads.size())
            {
                cur_read = short_reads[0];
                short_reads.erase(short_reads.begin(), short_reads.begin()+1);
                // reset eof flag
                if (!short_reads.size() && read_offset >= read_end)
                    in_eof = true;
            }
            else
            {
                cur_read = new dd_buf_t(read_offset, iinfo.in_seekable && read_offset + blocksize > read_end ? read_end-read_offset : blocksize);
                read_offset += cur_read->max;
                if (read_offset >= read_end)
                    in_eof = true;
            }
            ring_data_t *data = ((ring_data_t*)sqe->user_data);
            data->iov = (iovec){ cur_read->buf + cur_read->len, cur_read->max - cur_read->len };
            my_uring_prep_readv(sqe, iinfo.ifd, &data->iov, 1, iinfo.in_seekable ? iseek + cur_read->offset + cur_read->len : -1);
            in_waiting++;
            data->callback = [this, cur_read](ring_data_t *data)
            {
                in_waiting--;
                if (data->res < 0)
                {
                    fprintf(
                        stderr, "Failed to read %lu bytes from %s at offset %lu: %s (code %d)\n",
                        data->iov.iov_len, iinfo.ifile == "" ? "stdin" : iinfo.ifile.c_str(), cur_read->offset,
                        strerror(-data->res), data->res
                    );
                    if (!ignore_errors)
                    {
                        copy_error = -data->res;
                    }
                }
                else if (data->res == 0)
                {
                    in_eof = true;
                }
                if (data->res <= 0)
                {
                    if (cur_read->len > 0)
                        add_finished_read(cur_read);
                    else
                        delete cur_read;
                }
                else
                {
                    cur_read->len += data->res;
                    if (cur_read->len < cur_read->max)
                    {
                        // short read, retry
                        short_reads.push_back(cur_read);
                        // reset eof flag to signal that there's still something to read
                        in_eof = false;
                    }
                    else
                    {
                        add_finished_read(cur_read);
                    }
                }
                parent->ringloop->wakeup();
            };
        }
        return true;
    }

    void add_finished_read(dd_buf_t *cur_read)
    {
        if (!write_zero && is_zero(cur_read->buf, cur_read->max))
        {
            // do not write all-zero buffer
            delete cur_read;
            return;
        }
        auto it = std::lower_bound(read_buffers.begin(), read_buffers.end(), cur_read, [](dd_buf_t *item, dd_buf_t *ref)
        {
            return item->offset < ref->offset;
        });
        read_buffers.insert(it, cur_read);
    }

    bool add_write_op()
    {
        dd_buf_t *cur_read;
        if (short_writes.size())
        {
            cur_read = short_writes[0];
            short_writes.erase(short_writes.begin(), short_writes.begin()+1);
        }
        else
        {
            cur_read = read_buffers[0];
            if (!oinfo.out_seekable && cur_read->offset > written_size)
            {
                // can't write - input buffers are out of order
                return false;
            }
            cur_read->max = cur_read->len;
            cur_read->len = 0;
            read_buffers.erase(read_buffers.begin(), read_buffers.begin()+1);
        }
        if (oinfo.owatch)
        {
            cluster_op_t *write_op = new cluster_op_t;
            write_op->opcode = OSD_OP_WRITE;
            write_op->inode = oinfo.owatch->cfg.num;
            // FIXME: Support unaligned write?
            write_op->offset = cur_read->offset + oseek;
            write_op->len = round_up(cur_read->max, oinfo.out_granularity);
            write_op->iov.push_back(cur_read->buf, cur_read->max);
            if (cur_read->max < write_op->len)
            {
                // Zero pad
                write_op->iov.push_back(zero_buf.data(), write_op->len - cur_read->max);
            }
            out_waiting++;
            write_op->callback = [this, cur_read](cluster_op_t *write_op)
            {
                out_waiting--;
                if (write_op->retval != write_op->len)
                {
                    fprintf(
                        stderr, "Failed to write %lu bytes to image %s at offset %lu: %s (code %d)\n",
                        write_op->len, oinfo.oimg.c_str(), write_op->offset,
                        strerror(write_op->retval < 0 ? -write_op->retval : EIO), write_op->retval
                    );
                    if (!ignore_errors)
                    {
                        copy_error = write_op->retval < 0 ? -write_op->retval : EIO;
                    }
                }
                else
                {
                    written_size += write_op->len;
                }
                delete cur_read;
                delete write_op;
                parent->ringloop->wakeup();
            };
            parent->cli->execute(write_op);
        }
        else
        {
            io_uring_sqe *sqe = parent->ringloop->get_sqe();
            if (!sqe)
            {
                return false;
            }
            ring_data_t *data = ((ring_data_t*)sqe->user_data);
            data->iov = (iovec){ .iov_base = cur_read->buf+cur_read->len, .iov_len = cur_read->max-cur_read->len };
            my_uring_prep_writev(sqe, oinfo.ofd, &data->iov, 1, oinfo.out_seekable ? cur_read->offset+cur_read->len+oseek : -1);
            out_waiting++;
            data->callback = [this, cur_read](ring_data_t *data)
            {
                out_waiting--;
                if (data->res < 0)
                {
                    fprintf(
                        stderr, "Failed to write %lu bytes to %s at offset %lu: %s (code %d)\n",
                        data->iov.iov_len, oinfo.ofile == "" ? "stdout" : oinfo.ofile.c_str(),
                        oinfo.out_seekable ? cur_read->offset+cur_read->len+oseek : 0,
                        strerror(-data->res), data->res
                    );
                    if (!ignore_errors)
                    {
                        copy_error = -data->res;
                    }
                    delete cur_read;
                }
                else
                {
                    written_size += data->res;
                    cur_read->len += data->res;
                    if (cur_read->len < cur_read->max)
                        short_writes.push_back(cur_read);
                    else
                        delete cur_read;
                }
                parent->ringloop->wakeup();
            };
        }
        return true;
    }

    void print_progress(bool end)
    {
        if (!parent->progress && (!end || !end_status && !parent->json_output))
        {
            return;
        }
        timespec tv_now;
        clock_gettime(CLOCK_REALTIME, &tv_now);
        double sec_delta = ((tv_now.tv_sec - tv_progress.tv_sec) + (double)(tv_now.tv_nsec - tv_progress.tv_nsec)/1000000000.0);
        if (sec_delta < 1 && !end)
        {
            return;
        }
        double sec_total = ((tv_now.tv_sec - tv_begin.tv_sec) + (double)(tv_now.tv_nsec - tv_begin.tv_nsec)/1000000000.0);
        uint64_t delta = written_size-written_progress;
        tv_progress = tv_now;
        written_progress = written_size;
        if (end)
        {
            char buf[256];
            snprintf(
                buf, sizeof(buf), "%lu bytes (%s) copied, %.1f s, %sB/s",
                written_size, format_size(written_size).c_str(), sec_total,
                format_size((uint64_t)(written_size/sec_total), true).c_str()
            );
            if (parent->json_output)
            {
                if (parent->progress)
                    fprintf(stderr, "\n");
                result.text = buf;
                result.data = json11::Json::object {
                    { "copied", written_size },
                    { "seconds", sec_total },
                };
            }
            else
            {
                fprintf(stderr, (parent->progress ? ("\r%s\033[K\n") : ("%s\n")), buf);
            }
        }
        else
        {
            fprintf(
                stderr, "\r%lu bytes (%s) copied, %.1f s, %sB/s, avg %sB/s\033[K",
                written_size, format_size(written_size).c_str(), sec_total,
                format_size((uint64_t)(delta/sec_delta), true).c_str(),
                format_size((uint64_t)(written_size/sec_total), true).c_str()
            );
        }
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        else if (state == 4)
            goto resume_4;
        if ((oinfo.oimg != "" && oinfo.ofile != "") || (iinfo.iimg != "" && iinfo.ifile != ""))
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Image and file can't be specified at the same time" };
            state = 100;
            return;
        }
        if ((iinfo.iimg != "" ? "i"+iinfo.iimg : "f"+iinfo.ifile) == (oinfo.oimg != "" ? "i"+oinfo.oimg : "f"+oinfo.ofile))
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Input and output image/file can't be equal" };
            state = 100;
            return;
        }
        zero_buf.resize(blocksize);
        // Open input and output
        iinfo.open_input(parent);
        if (iinfo.result.err)
        {
            result = iinfo.result;
            state = 100;
            return;
        }
        if (iinfo.iwatch && ((iseek % iinfo.in_granularity) || (blocksize % iinfo.in_granularity)))
        {
            iinfo.close_input(parent);
            result = (cli_result_t){ .err = EINVAL, .text = "Unaligned read from Vitastor is not supported" };
            state = 100;
            return;
        }
        if (!oinfo.out_size)
        {
            oinfo.out_size = oseek + (iinfo.in_seekable && (!bytelimit || iinfo.in_size-iseek < bytelimit) ? iinfo.in_size-iseek : bytelimit);
        }
resume_1:
resume_2:
        if (!oinfo.open_output(parent, state, 1))
        {
            return;
        }
        if (oinfo.result.err)
        {
            iinfo.close_input(parent);
            result = oinfo.result;
            state = 100;
            return;
        }
        if (oinfo.owatch && ((oseek % oinfo.out_granularity) || (blocksize % oinfo.out_granularity)))
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Unaligned write to Vitastor is not supported" };
            goto close_end;
        }
        // Copy data
        if (iinfo.in_seekable && iseek >= iinfo.in_size)
        {
            result = (cli_result_t){ .err = -EINVAL, .text = "Input seek position is beyond end of input" };
            goto close_end;
        }
        if (!iinfo.iwatch && !iinfo.in_seekable && iseek)
        {
            // Read and ignore some data from input
            int res = skip_read(iinfo.ifd, iseek);
            if (res < 0)
            {
                result = (cli_result_t){ .err = -res, .text = "Failed to skip "+std::to_string(iseek)+" input bytes: "+std::string(strerror(-res)) };
                goto close_end;
            }
        }
        in_iodepth = iinfo.in_seekable ? iodepth : 1;
        out_iodepth = oinfo.out_seekable ? iodepth : 1;
        write_zero = write_zero || !oinfo.out_seekable;
        oinfo.end_fsync = oinfo.end_fsync && oinfo.out_seekable;
        read_offset = 0;
        read_end = iinfo.in_seekable ? iinfo.in_size-iseek : 0;
        if (bytelimit && (!read_end || read_end > bytelimit))
            read_end = bytelimit;
        clock_gettime(CLOCK_REALTIME, &tv_begin);
        tv_progress = tv_begin;
resume_3:
        while ((ignore_errors || !copy_error) && (!in_eof || read_buffers.size() || in_waiting > 0 || out_waiting > 0))
        {
            print_progress(false);
            while ((ignore_errors || !copy_error) &&
                (!in_eof && in_waiting < in_iodepth && read_buffers.size() < out_iodepth ||
                read_buffers.size() && out_waiting < out_iodepth))
            {
                if (!in_eof && in_waiting < in_iodepth && read_buffers.size() < out_iodepth)
                {
                    if (!add_read_op())
                    {
                        break;
                    }
                }
                if (read_buffers.size() && out_waiting < out_iodepth)
                {
                    if (!add_write_op())
                    {
                        break;
                    }
                }
            }
            if (in_waiting > 0 || out_waiting > 0)
            {
                state = 3;
                return;
            }
        }
        if (oinfo.end_fsync)
        {
resume_4:
            if (!oinfo.fsync_output(parent, state, 4))
            {
                return;
            }
        }
        print_progress(true);
close_end:
        oinfo.close_output(parent);
        iinfo.close_input(parent);
        // Done
        result.err = copy_error;
        state = 100;
    }
};

// parse <n>B or <n> blocks of size `bs`
static uint64_t parse_blocks(json11::Json v, uint64_t bs, uint64_t def)
{
    uint64_t res;
    if (!v.is_string() && !v.is_number() ||
        v.is_string() && v.string_value() == "" ||
        v.is_number() && !v.uint64_value())
        return def;
    auto num = v.uint64_value();
    if (num)
        return num * bs;
    auto s = v.string_value();
    if (s != "" && (s[s.size()-1] == 'b' || s[s.size()-1] == 'B'))
        res = stoull_full(s.substr(0, s.size()-1));
    else
        res = parse_size(s);
    return res;
}

std::function<bool(cli_result_t &)> cli_tool_t::start_dd(json11::Json cfg)
{
    auto dd = new cli_dd_t();
    dd->parent = this;
    dd->iinfo.iimg = cfg["iimg"].string_value();
    dd->oinfo.oimg = cfg["oimg"].string_value();
    dd->iinfo.ifile = cfg["if"].string_value();
    dd->oinfo.ofile = cfg["of"].string_value();
    dd->blocksize = parse_size(cfg["bs"].string_value());
    if (!dd->blocksize)
        dd->blocksize = 1048576;
    dd->bytelimit = parse_blocks(cfg["count"], dd->blocksize, 0);
    dd->oseek = parse_blocks(cfg["oseek"], dd->blocksize, 0);
    if (!dd->oseek)
        dd->oseek = parse_blocks(cfg["seek"], dd->blocksize, 0);
    dd->iseek = parse_blocks(cfg["oseek"], dd->blocksize, 0);
    if (!dd->iseek)
        dd->iseek = parse_blocks(cfg["skip"], dd->blocksize, 0);
    dd->iodepth = cfg["iodepth"].uint64_value();
    if (!dd->iodepth)
        dd->iodepth = 4;
    if (cfg["status"] == "none")
        dd->end_status = false;
    else if (cfg["status"] == "progress")
        progress = true;
    dd->iinfo.detect_size = cfg["size"].is_null();
    dd->oinfo.out_size = parse_size(cfg["size"].as_string());
    std::vector<std::string> conv = explode(",", cfg["conv"].string_value(), true);
    if (std::find(conv.begin(), conv.end(), "nofsync") != conv.end())
        dd->oinfo.end_fsync = false;
    if (std::find(conv.begin(), conv.end(), "trunc") != conv.end())
        dd->oinfo.out_trunc = true;
    if (std::find(conv.begin(), conv.end(), "nocreat") != conv.end())
        dd->oinfo.out_create = false;
    if (std::find(conv.begin(), conv.end(), "noerror") != conv.end())
        dd->ignore_errors = true;
    if (std::find(conv.begin(), conv.end(), "nosparse") != conv.end())
        dd->write_zero = true;
    conv = explode(",", cfg["iflag"].string_value(), true);
    if (std::find(conv.begin(), conv.end(), "direct") != conv.end())
        dd->iinfo.in_direct = true;
    conv = explode(",", cfg["oflag"].string_value(), true);
    if (std::find(conv.begin(), conv.end(), "direct") != conv.end())
        dd->oinfo.out_direct = true;
    if (std::find(conv.begin(), conv.end(), "append") != conv.end())
        dd->oinfo.out_append = true;
    return [dd](cli_result_t & result)
    {
        dd->loop();
        if (dd->is_done())
        {
            result = dd->result;
            delete dd;
            return true;
        }
        return false;
    };
}
