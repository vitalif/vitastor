// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

// FIO engine to test cluster I/O
//
// Random write:
//
// fio -thread -ioengine=./libfio_cluster.so -name=test -bs=4k -direct=1 -fsync=16 -iodepth=16 -rw=randwrite \
//     -etcd=127.0.0.1:2379 [-etcd_prefix=/vitastor] -pool=1 -inode=1 -size=1000M
//
// Linear write:
//
// fio -thread -ioengine=./libfio_cluster.so -name=test -bs=128k -direct=1 -fsync=32 -iodepth=32 -rw=write \
//     -etcd=127.0.0.1:2379 [-etcd_prefix=/vitastor] -pool=1 -inode=1 -size=1000M
//
// Random read (run with -iodepth=32 or -iodepth=1):
//
// fio -thread -ioengine=./libfio_cluster.so -name=test -bs=4k -direct=1 -iodepth=32 -rw=randread \
//     -etcd=127.0.0.1:2379 [-etcd_prefix=/vitastor] -pool=1 -inode=1 -size=1000M

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <vector>
#include <unordered_map>

#include "epoll_manager.h"
#include "cluster_client.h"
#include "fio_headers.h"

struct sec_data
{
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    bool last_sync = false;
    /* The list of completed io_u structs. */
    std::vector<io_u*> completed;
    uint64_t op_n = 0, inflight = 0;
    bool trace = false;
};

struct sec_options
{
    int __pad;
    char *etcd_host = NULL;
    char *etcd_prefix = NULL;
    uint64_t pool = 0;
    uint64_t inode = 0;
    int cluster_log = 0;
    int trace = 0;
};

static struct fio_option options[] = {
    {
        .name   = "etcd",
        .lname  = "etcd address",
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct sec_options, etcd_host),
        .help   = "etcd address in the form HOST:PORT[/PATH]",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "etcd",
        .lname  = "etcd key prefix",
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct sec_options, etcd_prefix),
        .help   = "etcd key prefix, by default /vitastor",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "pool",
        .lname  = "pool number for the inode",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, pool),
        .help   = "pool number for the inode to run tests on",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "inode",
        .lname  = "inode to run tests on",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, inode),
        .help   = "inode to run tests on (1 by default)",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "cluster_log_level",
        .lname  = "cluster log level",
        .type   = FIO_OPT_BOOL,
        .off1   = offsetof(struct sec_options, cluster_log),
        .help   = "Set log level for the Vitastor client",
        .def    = "0",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "osd_trace",
        .lname  = "OSD trace",
        .type   = FIO_OPT_BOOL,
        .off1   = offsetof(struct sec_options, trace),
        .help   = "Trace OSD operations",
        .def    = "0",
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

    bsd = new sec_data;
    if (!bsd)
    {
        td_verror(td, errno, "calloc");
        return 1;
    }
    td->io_ops_data = bsd;

    if (!td->files_index)
    {
        add_file(td, "osd_cluster", 0, 0);
        td->o.nr_files = td->o.nr_files ? : 1;
        td->o.open_files++;
    }

    return 0;
}

static void sec_cleanup(struct thread_data *td)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    if (bsd)
    {
        delete bsd->cli;
        delete bsd->epmgr;
        delete bsd->ringloop;
        bsd->cli = NULL;
        bsd->epmgr = NULL;
        bsd->ringloop = NULL;
    }
}

/* Connect to the server from each thread. */
static int sec_init(struct thread_data *td)
{
    sec_options *o = (sec_options*)td->eo;
    sec_data *bsd = (sec_data*)td->io_ops_data;

    json11::Json cfg = json11::Json::object {
        { "etcd_address", std::string(o->etcd_host) },
        { "etcd_prefix", std::string(o->etcd_prefix ? o->etcd_prefix : "/vitastor") },
        { "log_level", o->cluster_log },
    };

    if (o->pool)
        o->inode = (o->inode & ((1l << (64-POOL_ID_BITS)) - 1)) | (o->pool << (64-POOL_ID_BITS));
    if (!(o->inode >> (64-POOL_ID_BITS)))
    {
        td_verror(td, EINVAL, "pool is missing");
        return 1;
    }
    bsd->ringloop = new ring_loop_t(512);
    bsd->epmgr = new epoll_manager_t(bsd->ringloop);
    bsd->cli = new cluster_client_t(bsd->ringloop, bsd->epmgr->tfd, cfg);

    bsd->trace = o->trace ? true : false;

    return 0;
}

/* Begin read or write request. */
static enum fio_q_status sec_queue(struct thread_data *td, struct io_u *io)
{
    sec_options *opt = (sec_options*)td->eo;
    sec_data *bsd = (sec_data*)td->io_ops_data;
    int n = bsd->op_n;

    fio_ro_check(td, io);
    if (io->ddir == DDIR_SYNC && bsd->last_sync)
    {
        return FIO_Q_COMPLETED;
    }

    io->engine_data = bsd;
    cluster_op_t *op = new cluster_op_t;

    switch (io->ddir)
    {
    case DDIR_READ:
        op->opcode = OSD_OP_READ;
        op->inode = opt->inode;
        op->offset = io->offset;
        op->len = io->xfer_buflen;
        op->iov.push_back(io->xfer_buf, io->xfer_buflen);
        bsd->last_sync = false;
        break;
    case DDIR_WRITE:
        op->opcode = OSD_OP_WRITE;
        op->inode = opt->inode;
        op->offset = io->offset;
        op->len = io->xfer_buflen;
        op->iov.push_back(io->xfer_buf, io->xfer_buflen);
        bsd->last_sync = false;
        break;
    case DDIR_SYNC:
        op->opcode = OSD_OP_SYNC;
        bsd->last_sync = true;
        break;
    default:
        io->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

    op->callback = [io, n](cluster_op_t *op)
    {
        io->error = op->retval < 0 ? -op->retval : 0;
        sec_data *bsd = (sec_data*)io->engine_data;
        bsd->inflight--;
        bsd->completed.push_back(io);
        if (bsd->trace)
        {
            printf("--- %s n=%d retval=%d\n", io->ddir == DDIR_READ ? "READ" :
                (io->ddir == DDIR_WRITE ? "WRITE" : "SYNC"), n, op->retval);
        }
        delete op;
    };

    if (opt->trace)
    {
        if (io->ddir == DDIR_SYNC)
        {
            printf("+++ SYNC # %d\n", n);
        }
        else
        {
            printf("+++ %s # %d 0x%llx+%llx\n",
                io->ddir == DDIR_READ ? "READ" : "WRITE",
                n, io->offset, io->xfer_buflen);
        }
    }

    io->error = 0;
    bsd->inflight++;
    bsd->op_n++;
    bsd->cli->execute(op);

    if (io->error != 0)
        return FIO_Q_COMPLETED;
    return FIO_Q_QUEUED;
}

static int sec_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    sec_data *bsd = (sec_data*)td->io_ops_data;
    while (true)
    {
        bsd->ringloop->loop();
        if (bsd->completed.size() >= min)
            break;
        bsd->ringloop->wait();
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
    .name               = "vitastor_cluster",
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
