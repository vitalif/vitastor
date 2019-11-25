// FIO engine to test Blockstore

#include "blockstore.h"
#include "fio.h"

struct bs_data
{
    blockstore *bs;
    ring_loop_t *ringloop;
    /* The list of completed io_u structs. */
    std::vector<io_u*> completed;
};

struct bs_options
{
    char *data_device, *meta_device, *journal_device;
};

static struct fio_option options[] = {
    {
        .name   = "data_device",
        .lname  = "Data device",
        .help   = "Name of the data device",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
        .type   = FIO_OPT_STR_STORE,
        .off1   = offsetof(struct bs_options, data_device),
    },
    {
        .name = NULL,
    },
};

static int bs_setup(struct thread_data *td)
{
    bs_data *bsd;
    bs_options *o = td->eo;
    fio_file *f;
    int r;
    //int64_t size;

    bsd = (bs_data*)calloc(1, sizeof(*bsd));
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
    bs_data *bsd = td->io_ops_data;

    if (bsd)
    {
        
        free(bs_data);
    }
}

/* Connect to the server from each thread. */
static int bs_init(struct thread_data *td)
{
    struct bs_options *o = td->eo;
    struct bs_data *bs_data = td->io_ops_data;
    int r;

    spp::sparse_hash_map<std::string, std::string> config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    bsd->ringloop = new ring_loop_t(512);
    bsd->bs = new blockstore(config, ringloop);
    while (!bsd->bs->is_started())
    {
        bsd->ringloop->loop();
    }

    log_info("fio: blockstore initialized\n");
    return 0;
}

/* Begin read or write request. */
static enum fio_q_status bs_queue(struct thread_data *td, struct io_u *io_u)
{
    struct bs_data *bsd = td->io_ops_data;

    fio_ro_check(td, io_u);

    io_u->engine_data = bsd;

    if (io_u->ddir == DDIR_WRITE || io_u->ddir == DDIR_READ)
        assert(io_u->xfer_buflen <= bsd->block_size);

    blockstore_operation *op = new blockstore_operation;

    switch (io_u->ddir)
    {
    case DDIR_READ:
        op->flags = OP_READ;
        op->buf = io_u->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io_u->offset >> bsd->block_order,
        };
        op->offset = io_u->offset % bsd->block_size;
        op->len = io_u->xfer_buflen;
        op->callback = [&](blockstore_operation *op)
        {
            bsd->completed.push_back(io_u);
            delete op;
        };
        break;
    case DDIR_WRITE:
        op->flags = OP_WRITE;
        op->buf = io_u->xfer_buf;
        op->oid = {
            .inode = 1,
            .stripe = io_u->offset >> bsd->block_order,
        };
        op->offset = io_u->offset % bsd->block_size;
        op->len = io_u->xfer_buflen;
        op->callback = [&](blockstore_operation *op)
        {
            bsd->completed.push_back(io_u);
            delete op;
        };
        break;
    case DDIR_SYNC:
        op->flags = OP_SYNC;
        op->callback = [&](blockstore_operation *op)
        {
            if (bsd->bs->unstable_writes.size() > 0)
            {
                op->flags = OP_STABLE;
                op->len = bsd->bs->unstable_writes.size();
                op->buf = new obj_ver_id[op->len];
                int i = 0;
                for (auto it = bsd->bs->unstable_writes.begin(); it != bsd->bs->unstable_writes.end(); it++, i++)
                {
                    op->buf[i] = {
                        .oid = it->first,
                        .version = it->second,
                    };
                }
                bsd->bs->enqueue_op(op);
                op->callback = [&](blockstore_operation *op)
                {
                    bsd->completed.push_back(io_u);
                    delete[] op->buf;
                    delete op;
                };
            }
            else
            {
                bsd->completed.push_back(io_u);
                delete op;
            }
        };
        break;
    default:
        io_u->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

    bsd->bs->enqueue_op(op);

    io_u->error = 0;
    return FIO_Q_QUEUED;
}

static int bs_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    struct bs_data *bsd = td->io_ops_data;
    // FIXME timeout
    while (bsd->completed.size() < min)
    {
        bsd->ringloop->loop();
    }
    return bsd->completed.size();
}

static struct io_u *bs_event(struct thread_data *td, int event)
{
    struct bs_data *bsd = td->io_ops_data;
    if (bsd->completed.size() == 0)
        return NULL;
    /* FIXME We ignore the event number and assume fio calls us exactly once for [0..nr_events-1] */
    struct io_u *ev = bsd->completed.back();
    bsd->completed.pop_back();
    return ev;
}

static int bs_io_u_init(struct thread_data *td, struct io_u *io_u)
{
    io_u->engine_data = NULL;
    return 0;
}

static void bs_io_u_free(struct thread_data *td, struct io_u *io_u)
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

static struct ioengine_ops ioengine = {
    .name               = "microceph_blockstore",
    .version            = FIO_IOOPS_VERSION,
    .options            = options,
    .option_struct_size = sizeof(struct bs_options),
    .flags              = FIO_MEMALIGN | FIO_DISKLESSIO | FIO_NOEXTEND,

    .setup              = bs_setup,
    .init               = bs_init,
    .cleanup            = bs_cleanup,
    .queue              = bs_queue,
    .getevents          = bs_getevents,
    .event              = bs_event,
    .io_u_init          = bs_io_u_init,
    .io_u_free          = bs_io_u_free,

    .open_file          = bs_open_file,
    .invalidate         = bs_invalidate,
};

static void fio_init fio_bs_register(void)
{
    register_ioengine(&ioengine);
}

static void fio_exit fio_bs_unregister(void)
{
    unregister_ioengine(&ioengine);
}
