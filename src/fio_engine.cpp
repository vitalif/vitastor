// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

// FIO engine to test Blockstore
//
// Initialize storage for tests:
//
// dd if=/dev/zero of=test_data.bin bs=1024 count=1048576
// dd if=/dev/zero of=test_meta.bin bs=1024 count=256
// dd if=/dev/zero of=test_journal.bin bs=1024 count=4096
//
// Random write:
//
// fio -thread -ioengine=./libfio_blockstore.so -name=test -bs=4k -direct=1 -fsync=16 -iodepth=16 -rw=randwrite \
//     -bs_config='{"data_device":"./test_data.bin"}' -size=1000M
//
// Linear write:
//
// fio -thread -ioengine=./libfio_blockstore.so -name=test -bs=128k -direct=1 -fsync=32 -iodepth=32 -rw=write \
//     -bs_config='{"data_device":"./test_data.bin"}' -size=1000M
//
// Random read (run with -iodepth=32 or -iodepth=1):
//
// fio -thread -ioengine=./libfio_blockstore.so -name=test -bs=4k -direct=1 -iodepth=32 -rw=randread \
//     -bs_config='{"data_device":"./test_data.bin"}' -size=1000M

#include "blockstore.h"
#include "epoll_manager.h"
#include "fio_headers.h"

#include "json11/json11.hpp"

struct bs_data
{
    blockstore_t *bs;
    epoll_manager_t *epmgr;
    ring_loop_t *ringloop;
    /* The list of completed io_u structs. */
    std::vector<io_u*> completed;
    int op_n = 0, inflight = 0;
    bool last_sync = false;
};

struct bs_options
{
    int __pad;
    char *json_config = NULL;
};

static struct fio_option options[] = {
    {
        .name   = "bs_config",
        .lname  = "JSON config for Blockstore",
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct bs_options, json_config),
        .help   = "JSON config for Blockstore",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name = NULL,
    },
};

static int bs_setup(struct thread_data *td)
{
    bs_data *bsd;
    //fio_file *f;
    //int r;
    //int64_t size;

    bsd = new bs_data;
    if (!bsd)
    {
        td_verror(td, errno, "calloc");
        return 1;
    }
    td->io_ops_data = bsd;

    if (!td->files_index)
    {
        add_file(td, "blockstore", 0, 0);
        td->o.nr_files = td->o.nr_files ? : 1;
        td->o.open_files++;
    }

    //f = td->files[0];
    //f->real_file_size = size;
    return 0;
}

static void bs_cleanup(struct thread_data *td)
{
    bs_data *bsd = (bs_data*)td->io_ops_data;
    if (bsd)
    {
        while (1)
        {
            do
            {
                bsd->ringloop->loop();
                if (bsd->bs->is_safe_to_stop())
                    goto safe;
            } while (bsd->ringloop->has_work());
            bsd->ringloop->wait();
        }
    safe:
        delete bsd->bs;
        delete bsd->epmgr;
        delete bsd->ringloop;
        delete bsd;
    }
}

/* Connect to the server from each thread. */
static int bs_init(struct thread_data *td)
{
    bs_options *o = (bs_options*)td->eo;
    bs_data *bsd = (bs_data*)td->io_ops_data;

    blockstore_config_t config;
    if (o->json_config)
    {
        std::string json_err;
        auto json_cfg = json11::Json::parse(o->json_config, json_err);
        for (auto p: json_cfg.object_items())
        {
            if (p.second.is_string())
                config[p.first] = p.second.string_value();
            else
                config[p.first] = p.second.dump();
        }
    }
    bsd->ringloop = new ring_loop_t(512);
    bsd->epmgr = new epoll_manager_t(bsd->ringloop);
    bsd->bs = new blockstore_t(config, bsd->ringloop, bsd->epmgr->tfd);
    while (1)
    {
        bsd->ringloop->loop();
        if (bsd->bs->is_started())
            break;
        bsd->ringloop->wait();
    }

    log_info("fio: blockstore initialized\n");
    return 0;
}

/* Begin read or write request. */
static enum fio_q_status bs_queue(struct thread_data *td, struct io_u *io)
{
    bs_data *bsd = (bs_data*)td->io_ops_data;
    int n = bsd->op_n;
    if (io->ddir == DDIR_SYNC && bsd->last_sync)
    {
        return FIO_Q_COMPLETED;
    }

    fio_ro_check(td, io);

    io->engine_data = bsd;

    if (io->ddir == DDIR_WRITE || io->ddir == DDIR_READ)
        assert(io->xfer_buflen <= bsd->bs->get_block_size());

    blockstore_op_t *op = new blockstore_op_t;
    op->callback = NULL;

    switch (io->ddir)
    {
    case DDIR_READ:
        op->opcode = BS_OP_READ;
        op->buf = io->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io->offset / bsd->bs->get_block_size(),
        };
        op->version = UINT64_MAX; // last unstable
        op->offset = io->offset % bsd->bs->get_block_size();
        op->len = io->xfer_buflen;
        op->callback = [io, n](blockstore_op_t *op)
        {
            io->error = op->retval < 0 ? -op->retval : 0;
            bs_data *bsd = (bs_data*)io->engine_data;
            bsd->inflight--;
            bsd->completed.push_back(io);
#ifdef BLOCKSTORE_DEBUG
            printf("--- OP_READ %llx n=%d retval=%d\n", io, n, op->retval);
#endif
            delete op;
        };
        break;
    case DDIR_WRITE:
        op->opcode = BS_OP_WRITE;
        op->buf = io->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io->offset / bsd->bs->get_block_size(),
        };
        op->version = 0; // assign automatically
        op->offset = io->offset % bsd->bs->get_block_size();
        op->len = io->xfer_buflen;
        op->callback = [io, n](blockstore_op_t *op)
        {
            io->error = op->retval < 0 ? -op->retval : 0;
            bs_data *bsd = (bs_data*)io->engine_data;
            bsd->inflight--;
            bsd->completed.push_back(io);
#ifdef BLOCKSTORE_DEBUG
            printf("--- OP_WRITE %llx n=%d retval=%d\n", io, n, op->retval);
#endif
            delete op;
        };
        bsd->last_sync = false;
        break;
    case DDIR_SYNC:
        op->opcode = BS_OP_SYNC_STAB_ALL;
        op->callback = [io, n](blockstore_op_t *op)
        {
            bs_data *bsd = (bs_data*)io->engine_data;
            io->error = op->retval < 0 ? -op->retval : 0;
            bsd->completed.push_back(io);
            bsd->inflight--;
#ifdef BLOCKSTORE_DEBUG
            printf("--- OP_SYNC %llx n=%d retval=%d\n", io, n, op->retval);
#endif
            delete op;
        };
        bsd->last_sync = true;
        break;
    default:
        io->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

#ifdef BLOCKSTORE_DEBUG
    printf("+++ %s %llx n=%d\n", op->opcode == OP_READ ? "OP_READ" : (op->opcode == OP_WRITE ? "OP_WRITE" : "OP_SYNC"), io, n);
#endif
    io->error = 0;
    bsd->inflight++;
    bsd->bs->enqueue_op(op);
    bsd->op_n++;

    if (io->error != 0)
        return FIO_Q_COMPLETED;
    return FIO_Q_QUEUED;
}

static int bs_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    bs_data *bsd = (bs_data*)td->io_ops_data;
    // FIXME timeout
    while (true)
    {
        bsd->ringloop->loop();
        if (bsd->completed.size() >= min)
            break;
        bsd->ringloop->wait();
    }
    return bsd->completed.size();
}

static struct io_u *bs_event(struct thread_data *td, int event)
{
    bs_data *bsd = (bs_data*)td->io_ops_data;
    if (bsd->completed.size() == 0)
        return NULL;
    /* FIXME We ignore the event number and assume fio calls us exactly once for [0..nr_events-1] */
    struct io_u *ev = bsd->completed.back();
    bsd->completed.pop_back();
    return ev;
}

static int bs_io_u_init(struct thread_data *td, struct io_u *io)
{
    io->engine_data = NULL;
    return 0;
}

static void bs_io_u_free(struct thread_data *td, struct io_u *io)
{
}

static int bs_open_file(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

static int bs_invalidate(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

struct ioengine_ops ioengine = {
    .name               = "vitastor_blockstore",
    .version            = FIO_IOOPS_VERSION,
    .flags              = FIO_MEMALIGN | FIO_DISKLESSIO | FIO_NOEXTEND,
    .setup              = bs_setup,
    .init               = bs_init,
    .queue              = bs_queue,
    .getevents          = bs_getevents,
    .event              = bs_event,
    .cleanup            = bs_cleanup,
    .open_file          = bs_open_file,
    .invalidate         = bs_invalidate,
    .io_u_init          = bs_io_u_init,
    .io_u_free          = bs_io_u_free,
    .option_struct_size = sizeof(struct bs_options),
    .options            = options,
};

static void fio_init fio_bs_register(void)
{
    register_ioengine(&ioengine);
}

static void fio_exit fio_bs_unregister(void)
{
    unregister_ioengine(&ioengine);
}
