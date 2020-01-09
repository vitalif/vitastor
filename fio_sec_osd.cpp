// FIO engine to test Blockstore through Secondary OSD interface
//
// Prepare storage like in fio_engine.cpp, then start OSD with ./osd, then test it
//
// Random write:
//
// fio -thread -ioengine=./libfio_sec_osd.so -name=test -bs=4k -direct=1 -fsync=16 -iodepth=16 -rw=randwrite \
//     -host=127.0.0.1 -port=11203 -size=1000M
//
// Linear write:
//
// fio -thread -ioengine=./libfio_sec_osd.so -name=test -bs=128k -direct=1 -fsync=32 -iodepth=32 -rw=write \
//     -host=127.0.0.1 -port=11203 -size=1000M
//
// Random read (run with -iodepth=32 or -iodepth=1):
//
// fio -thread -ioengine=./libfio_sec_osd.so -name=test -bs=4k -direct=1 -iodepth=32 -rw=randread \
//     -host=127.0.0.1 -port=11203 -size=1000M

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <vector>
#include <unordered_map>

#include "osd_ops.h"
extern "C" {
#define CONFIG_PWRITEV2
#include "fio/fio.h"
#include "fio/optgroup.h"
}

struct sec_data
{
    int connect_fd;
    /* block_size = 1 << block_order (128KB by default) */
    uint64_t block_order = 17, block_size = 1 << 17;
    std::unordered_map<uint64_t, io_u*> queue;
    /* The list of completed io_u structs. */
    std::vector<io_u*> completed;
    uint64_t op_n = 0, inflight = 0;
};

struct sec_options
{
    int __pad;
    char *host = NULL;
    int port = 0;
};

static struct fio_option options[] = {
    {
        .name   = "host",
        .lname  = "Test Secondary OSD host",
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct sec_options, host),
        .help   = "Test Secondary OSD host",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "port",
        .lname  = "Test Secondary OSD port",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, port),
        .help   = "Test Secondary OSD port",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name = NULL,
    },
};

static int sec_setup(struct thread_data *td)
{
    sec_data *bsd;
    //fio_file *f;
    //int r;
    //int64_t size;

    bsd = new sec_data;
    if (!bsd)
    {
        td_verror(td, errno, "calloc");
        return 1;
    }
    td->io_ops_data = bsd;

    if (!td->files_index)
    {
        add_file(td, "bs_sec_osd", 0, 0);
        td->o.nr_files = td->o.nr_files ? : 1;
        td->o.open_files++;
    }

    //f = td->files[0];
    //f->real_file_size = size;
    return 0;
}

static void sec_cleanup(struct thread_data *td)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    if (bsd)
    {
        close(bsd->connect_fd);
    }
}

/* Connect to the server from each thread. */
static int sec_init(struct thread_data *td)
{
    sec_options *o = (sec_options*)td->eo;
    sec_data *bsd = (sec_data*)td->io_ops_data;

    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, o->host ? o->host : "127.0.0.1", &addr.sin_addr)) != 1)
    {
        fprintf(stderr, "server address: %s%s\n", o->host ? o->host : "127.0.0.1", r == 0 ? " is not valid" : ": no ipv4 support");
        return 1;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(o->port ? o->port : 11203);

    bsd->connect_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (bsd->connect_fd < 0)
    {
        perror("socket");
        return 1;
    }
    if (connect(bsd->connect_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        perror("connect");
        return 1;
    }
    int one = 1;
    setsockopt(bsd->connect_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));

    // FIXME: read config (block size) from OSD

    return 0;
}

/* Begin read or write request. */
static enum fio_q_status sec_queue(struct thread_data *td, struct io_u *io)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    int n = bsd->op_n;

    fio_ro_check(td, io);

    io->engine_data = bsd;
    union
    {
        osd_any_op_t op;
        uint8_t op_buf[OSD_PACKET_SIZE] = { 0 };
    };

    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = n;
    switch (io->ddir)
    {
    case DDIR_READ:
        op.hdr.opcode = OSD_OP_SECONDARY_READ;
        op.sec_rw.oid = {
            .inode = 1,
            .stripe = io->offset >> bsd->block_order,
        };
        op.sec_rw.version = UINT64_MAX; // last unstable
        op.sec_rw.offset = io->offset % bsd->block_size;
        op.sec_rw.len = io->xfer_buflen;
        break;
    case DDIR_WRITE:
        op.hdr.opcode = OSD_OP_SECONDARY_WRITE;
        op.sec_rw.oid = {
            .inode = 1,
            .stripe = io->offset >> bsd->block_order,
        };
        op.sec_rw.version = 0; // assign automatically
        op.sec_rw.offset = io->offset % bsd->block_size;
        op.sec_rw.len = io->xfer_buflen;
        break;
    case DDIR_SYNC:
        // Allowed only for testing: sync & stabilize all unstable object versions
        op.hdr.opcode = OSD_OP_TEST_SYNC_STAB_ALL;
        break;
    default:
        io->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

    io->error = 0;
    bsd->inflight++;
    bsd->op_n++;
    bsd->queue[n] = io;

    if (write(bsd->connect_fd, op_buf, OSD_PACKET_SIZE) != OSD_PACKET_SIZE)
    {
        perror("write");
        exit(1);
    }
    if (io->ddir == DDIR_WRITE)
    {
        // Send data
        void *send_buf = io->xfer_buf;
        size_t remaining = io->xfer_buflen;
        while (remaining > 0)
        {
            size_t r = write(bsd->connect_fd, send_buf, remaining);
            if (r < 0)
            {
                if (r != EAGAIN)
                {
                    perror("write");
                    exit(1);
                }
                continue;
            }
            remaining -= r;
            send_buf += r;
        }
    }

    if (io->error != 0)
        return FIO_Q_COMPLETED;
    return FIO_Q_QUEUED;
}

void read_blocking(int fd, void *read_buf, size_t remaining)
{
    while (remaining > 0)
    {
        size_t r = read(fd, read_buf, remaining);
        if (r <= 0)
        {
            perror("read");
            exit(1);
        }
        remaining -= r;
        read_buf += r;
    }
}

static int sec_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    // FIXME timeout, at least poll. Now it's the stupidest implementation possible
    union
    {
        osd_any_reply_t reply;
        uint8_t reply_buf[OSD_PACKET_SIZE] = { 0 };
    };
    while (bsd->completed.size() < min)
    {
        read_blocking(bsd->connect_fd, reply_buf, OSD_PACKET_SIZE);
        if (reply.hdr.magic != SECONDARY_OSD_REPLY_MAGIC)
        {
            fprintf(stderr, "bad reply: magic = %lx instead of %lx\n", reply.hdr.magic, SECONDARY_OSD_REPLY_MAGIC);
            exit(1);
        }
        auto it = bsd->queue.find(reply.hdr.id);
        if (it == bsd->queue.end())
        {
            fprintf(stderr, "bad reply: op id %lx missing in local queue\n", reply.hdr.id);
            exit(1);
        }
        io_u* io = it->second;
        if (io->ddir == DDIR_READ)
        {
            if (reply.hdr.retval != io->xfer_buflen)
            {
                fprintf(stderr, "Short read: retval = %ld instead of %llu\n", reply.hdr.retval, io->xfer_buflen);
                exit(1);
            }
            read_blocking(bsd->connect_fd, io->xfer_buf, io->xfer_buflen);
        }
        else if (io->ddir == DDIR_WRITE)
        {
            if (reply.hdr.retval != io->xfer_buflen)
            {
                fprintf(stderr, "Short write: retval = %ld instead of %llu\n", reply.hdr.retval, io->xfer_buflen);
                exit(1);
            }
        }
        else if (io->ddir == DDIR_SYNC)
        {
            if (reply.hdr.retval != 0)
            {
                fprintf(stderr, "Sync failed: retval = %ld\n", reply.hdr.retval);
                exit(1);
            }
        }
        bsd->completed.push_back(io);
    }
    return bsd->completed.size();
}

static struct io_u *sec_event(struct thread_data *td, int event)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    if (bsd->completed.size() == 0)
        return NULL;
    /* FIXME We ignore the event number and assume fio calls us exactly once for [0..nr_events-1] */
    struct io_u *ev = bsd->completed.back();
    bsd->completed.pop_back();
    return ev;
}

static int sec_io_u_init(struct thread_data *td, struct io_u *io)
{
    io->engine_data = NULL;
    return 0;
}

static void sec_io_u_free(struct thread_data *td, struct io_u *io)
{
}

static int sec_open_file(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

static int sec_invalidate(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

struct ioengine_ops ioengine = {
    .name               = "microceph_secondary_osd",
    .version            = FIO_IOOPS_VERSION,
    .flags              = FIO_MEMALIGN | FIO_DISKLESSIO | FIO_NOEXTEND,
    .setup              = sec_setup,
    .init               = sec_init,
    .queue              = sec_queue,
    .getevents          = sec_getevents,
    .event              = sec_event,
    .cleanup            = sec_cleanup,
    .open_file          = sec_open_file,
    .invalidate         = sec_invalidate,
    .io_u_init          = sec_io_u_init,
    .io_u_free          = sec_io_u_free,
    .option_struct_size = sizeof(struct sec_options),
    .options            = options,
};

static void fio_init fio_sec_register(void)
{
    register_ioengine(&ioengine);
}

static void fio_exit fio_sec_unregister(void)
{
    unregister_ioengine(&ioengine);
}
