// FIO engine to test Blockstore

#include "blockstore.h"
extern "C" {
#define CONFIG_PWRITEV2
#include "fio/fio.h"
#include "fio/optgroup.h"
}

static const int DEBUG = 0;

struct bs_data
{
    blockstore *bs;
    ring_loop_t *ringloop;
    /* The list of completed io_u structs. */
    std::vector<io_u*> completed;
    int op_n = 0, inflight = 0;
};

struct bs_options
{
    char *data_device, *meta_device, *journal_device;
};

static struct fio_option options[] = {
    {
        .name   = "data_device",
        .lname  = "Data device",
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct bs_options, data_device),
        .help   = "Name of the data device",
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
    bs_options *o = (bs_options*)td->eo;
    fio_file *f;
    int r;
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
    f = td->files[0];

    //f->real_file_size = size;

    return 0;
}

static void bs_cleanup(struct thread_data *td)
{
    bs_data *bsd = (bs_data*)td->io_ops_data;

    if (bsd)
    {
        
        delete bsd;
    }
}

/* Connect to the server from each thread. */
static int bs_init(struct thread_data *td)
{
    bs_options *o = (bs_options*)td->eo;
    bs_data *bsd = (bs_data*)td->io_ops_data;
    int r;

    spp::sparse_hash_map<std::string, std::string> config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    bsd->ringloop = new ring_loop_t(512);
    bsd->bs = new blockstore(config, bsd->ringloop);
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

    fio_ro_check(td, io);

    io->engine_data = bsd;

    if (io->ddir == DDIR_WRITE || io->ddir == DDIR_READ)
        assert(io->xfer_buflen <= bsd->bs->block_size);

    blockstore_operation *op = new blockstore_operation;
    op->callback = NULL;

    switch (io->ddir)
    {
    case DDIR_READ:
        op->flags = OP_READ;
        op->buf = io->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io->offset >> bsd->bs->block_order,
        };
        op->offset = io->offset % bsd->bs->block_size;
        op->len = io->xfer_buflen;
        op->callback = [io](blockstore_operation *op)
        {
            io->error = op->retval < 0 ? -op->retval : 0;
            bs_data *bsd = (bs_data*)io->engine_data;
            bsd->completed.push_back(io);
            delete op;
        };
        break;
    case DDIR_WRITE:
        op->flags = OP_WRITE;
        op->buf = io->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io->offset >> bsd->bs->block_order,
        };
        op->offset = io->offset % bsd->bs->block_size;
        op->len = io->xfer_buflen;
        op->callback = [io, n](blockstore_operation *op)
        {
            io->error = op->retval < 0 ? -op->retval : 0;
            bs_data *bsd = (bs_data*)io->engine_data;
            bsd->inflight--;
            bsd->completed.push_back(io);
            if (DEBUG)
                printf("--- OP_WRITE %llx n=%d retval=%d\n", io, n, op->retval);
            delete op;
        };
        break;
    case DDIR_SYNC:
        op->flags = OP_SYNC;
        op->callback = [io, n](blockstore_operation *op)
        {
            bs_data *bsd = (bs_data*)io->engine_data;
            if (op->retval >= 0 && bsd->bs->unstable_writes.size() > 0)
            {
                op->flags = OP_STABLE;
                op->len = bsd->bs->unstable_writes.size();
                obj_ver_id *vers = new obj_ver_id[op->len];
                op->buf = vers;
                int i = 0;
                for (auto it = bsd->bs->unstable_writes.begin(); it != bsd->bs->unstable_writes.end(); it++, i++)
                {
                    vers[i] = {
                        .oid = it->first,
                        .version = it->second,
                    };
                }
                bsd->bs->enqueue_op(op);
                op->callback = [io, n](blockstore_operation *op)
                {
                    io->error = op->retval < 0 ? -op->retval : 0;
                    bs_data *bsd = (bs_data*)io->engine_data;
                    bsd->completed.push_back(io);
                    bsd->inflight--;
                    obj_ver_id *vers = (obj_ver_id*)op->buf;
                    delete[] vers;
                    if (DEBUG)
                        printf("--- OP_SYNC %llx n=%d retval=%d\n", io, n, op->retval);
                    delete op;
                };
            }
            else
            {
                io->error = op->retval < 0 ? -op->retval : 0;
                bsd->completed.push_back(io);
                bsd->inflight--;
                if (DEBUG)
                    printf("--- OP_SYNC %llx n=%d retval=%d\n", io, n, op->retval);
                delete op;
            }
        };
        break;
    default:
        io->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

    if (DEBUG)
        printf("+++ %s %llx\n", op->flags == OP_WRITE ? "OP_WRITE" : "OP_SYNC", io);
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
    .name               = "microceph_blockstore",
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
};

static void fio_init fio_bs_register(void)
{
    register_ioengine(&ioengine);
}

static void fio_exit fio_bs_unregister(void)
{
    unregister_ioengine(&ioengine);
}
