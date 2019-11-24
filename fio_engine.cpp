// FIO engine to test Blockstore

#include "blockstore.h"
#include "fio.h"

struct bs_data
{
    blockstore *bs;
    ring_loop_t *ringloop;

    /* The list of completed io_u structs. */
    struct io_u **completed;
    size_t nr_completed;
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

/* A command in flight has been completed. */
static int cmd_completed(void *vp, int *error)
{
    struct io_u *io_u;
    struct bs_data *bs_data;
    struct io_u **completed;

    io_u = vp;
    bs_data = io_u->engine_data;

    if (bs_data->debug)
        log_info("fio: nbd: command completed\n");

    if (*error != 0)
        io_u->error = *error;
    else
        io_u->error = 0;

    /* Add this completion to the list so it can be picked up
     * later by ->event.
     */
    completed = realloc(bs_data->completed,
                sizeof(struct io_u *) *
                (bs_data->nr_completed+1));
    if (completed == NULL) {
        io_u->error = errno;
        return 0;
    }

    bs_data->completed = completed;
    bs_data->completed[bs_data->nr_completed] = io_u;
    bs_data->nr_completed++;

    return 0;
}

/* Begin read or write request. */
static enum fio_q_status bs_queue(struct thread_data *td, struct io_u *io_u)
{
    struct bs_data *bsd = td->io_ops_data;
    bs_completion_callback completion = { .callback = cmd_completed, .user_data = io_u };
    int r;

    fio_ro_check(td, io_u);

    io_u->engine_data = bsd;

    if (io_u->ddir == DDIR_WRITE || io_u->ddir == DDIR_READ)
        assert(io_u->xfer_buflen <= bsd->block_size);

    switch (io_u->ddir) {
    case DDIR_READ:
        r = bs_aio_pread(bs_data->nbd,
                  io_u->xfer_buf, io_u->xfer_buflen,
                  io_u->offset, completion, 0);
        break;
    case DDIR_WRITE:
        r = bs_aio_pwrite(bs_data->nbd,
                   io_u->xfer_buf, io_u->xfer_buflen,
                   io_u->offset, completion, 0);
        break;
    case DDIR_TRIM:
        r = bs_aio_trim(bs_data->nbd, io_u->xfer_buflen,
                 io_u->offset, completion, 0);
        break;
    case DDIR_SYNC:
        /* XXX We could probably also handle
         * DDIR_SYNC_FILE_RANGE with a bit of effort.
         */
        r = bs_aio_flush(bs_data->nbd, completion, 0);
        break;
    default:
        io_u->error = EINVAL;
        return FIO_Q_COMPLETED;
    }

    if (r == -1) {
        /* errno is optional information on libnbd error path;
         * if it's 0, set it to a default value
         */
        io_u->error = bs_get_errno();
        if (io_u->error == 0)
            io_u->error = EIO;
        return FIO_Q_COMPLETED;
    }

    if (bs_data->debug)
        log_info("fio: nbd: command issued\n");
    io_u->error = 0;
    return FIO_Q_QUEUED;
}

static int bs_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    struct bs_data *bs_data = td->io_ops_data;
    int r;
    unsigned events = 0;
    int timeout;

    /* XXX This handling of timeout is wrong because it will wait
     * for up to loop iterations * timeout.
     */
    timeout = !t ? -1 : t->tv_sec * 1000 + t->tv_nsec / 1000000;

    while (events < min) {
        r = bs_poll(bs_data->nbd, timeout);
        if (r == -1) {
            /* error in poll */
            log_err("fio: bs_poll: %s\n", bs_get_error());
            return -1;
        }
        else {
            /* poll made progress */
            events += retire_commands(bs_data->nbd);
        }
    }

    return events;
}

static struct io_u *bs_event(struct thread_data *td, int event)
{
    struct bs_data *bs_data = td->io_ops_data;

    if (bs_data->nr_completed == 0)
        return NULL;

    /* XXX We ignore the event number and assume fio calls us
     * exactly once for [0..nr_events-1].
     */
    bs_data->nr_completed--;
    return bs_data->completed[bs_data->nr_completed];
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
