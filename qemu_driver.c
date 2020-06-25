// QEMU block driver

#define _GNU_SOURCE
#include "qemu/osdep.h"
#include "qemu/units.h"
#include "block/block_int.h"
#include "block/qdict.h"
#include "qapi/error.h"
#include "qapi/qmp/qdict.h"
#include "qapi/qmp/qerror.h"
#include "qemu/uri.h"
#include "qemu/error-report.h"
#include "qemu/module.h"
#include "qemu/option.h"
#include "qemu/cutils.h"

#include "qemu_proxy.h"

typedef struct FalconClient
{
    void *proxy;
    const char *etcd_host;
    const char *etcd_prefix;
    uint64_t inode;
    uint64_t size;
    int readonly;
    QemuMutex mutex;
} FalconClient;

typedef struct FalconRPC
{
    BlockDriverState *bs;
    Coroutine *co;
    QEMUIOVector *iov;
    int ret;
    int complete;
} FalconRPC;

static char *qemu_rbd_next_tok(char *src, char delim, char **p)
{
    char *end;
    *p = NULL;
    for (end = src; *end; ++end)
    {
        if (*end == delim)
            break;
        if (*end == '\\' && end[1] != '\0')
            end++;
    }
    if (*end == delim)
    {
        *p = end + 1;
        *end = '\0';
    }
    return src;
}

static void qemu_rbd_unescape(char *src)
{
    char *p;
    for (p = src; *src; ++src, ++p)
    {
        if (*src == '\\' && src[1] != '\0')
            src++;
        *p = *src;
    }
    *p = '\0';
}

// falcon[:key=value]*
// falcon:etcd_host=127.0.0.1:inode=1
static void falcon_parse_filename(const char *filename, QDict *options, Error **errp)
{
    const char *start;
    char *p, *buf;

    if (!strstart(filename, "falcon:", &start))
    {
        error_setg(errp, "File name must start with 'falcon:'");
        return;
    }

    buf = g_strdup(start);
    p = buf;

    // The following are all key/value pairs
    while (p)
    {
        char *name, *value;
        name = qemu_rbd_next_tok(p, '=', &p);
        if (!p)
        {
            error_setg(errp, "conf option %s has no value", name);
            break;
        }
        qemu_rbd_unescape(name);
        value = qemu_rbd_next_tok(p, ':', &p);
        qemu_rbd_unescape(value);
        if (!strcmp(name, "inode") || !strcmp(name, "size"))
        {
            unsigned long long num_val;
            if (parse_uint_full(value, &num_val, 0))
            {
                error_setg(errp, "Illegal %s: %s", name, value);
                goto out;
            }
            qdict_put_int(options, name, num_val);
        }
        else
        {
            qdict_put_str(options, name, value);
        }
    }
    if (!qdict_get_int(options, "inode"))
    {
        error_setg(errp, "inode is missing");
        goto out;
    }
    if (!qdict_get_int(options, "size"))
    {
        error_setg(errp, "size is missing");
        goto out;
    }
    if (!qdict_get_int(options, "etcd_host"))
    {
        error_setg(errp, "etcd_host is missing");
        goto out;
    }

out:
    g_free(buf);
    return;
}

static int falcon_file_open(BlockDriverState *bs, QDict *options, int flags, Error **errp)
{
    FalconClient *client = bs->opaque;
    int64_t ret = 0;
    client->etcd_host = qdict_get_try_str(options, "etcd_host");
    client->etcd_prefix = qdict_get_try_str(options, "etcd_prefix");
    client->inode = qdict_get_int(options, "inode");
    client->size = qdict_get_int(options, "size");
    client->readonly = (flags & BDRV_O_RDWR) ? 1 : 0;
    client->proxy = falcon_proxy_create(client->etcd_host, client->etcd_prefix);
    //client->aio_context = bdrv_get_aio_context(bs);
    bs->total_sectors = client->size / BDRV_SECTOR_SIZE;
    return ret;
}

static void falcon_close(BlockDriverState *bs)
{
    FalconClient *client = bs->opaque;
    falcon_proxy_destroy(client->proxy);
}

static int falcon_probe_blocksizes(BlockDriverState *bs, BlockSizes *bsz)
{
    bsz->phys = 4096;
    bsz->log = 4096;
    return 0;
}

static int coroutine_fn falcon_co_create_opts(BlockDriver *drv, const char *url, QemuOpts *opts, Error **errp)
{
    QDict *options;
    int ret;

    options = qdict_new();
    falcon_parse_filename(url, options, errp);
    if (errp)
    {
        ret = -1;
        goto out;
    }

    // inodes don't require creation in Falcon. FIXME: They will when there will be some metadata

    ret = 0;
out:
    qobject_unref(options);
    return ret;
}

static int coroutine_fn falcon_co_truncate(BlockDriverState *bs, int64_t offset, bool exact, PreallocMode prealloc, Error **errp)
{
    FalconClient *client = bs->opaque;

    if (prealloc != PREALLOC_MODE_OFF)
    {
        error_setg(errp, "Unsupported preallocation mode '%s'", PreallocMode_str(prealloc));
        return -ENOTSUP;
    }

    // TODO: Resize inode to <offset> bytes
    client->size = offset / BDRV_SECTOR_SIZE;

    return 0;
}

static int falcon_get_info(BlockDriverState *bs, BlockDriverInfo *bdi)
{
    bdi->cluster_size = 4096;
    return 0;
}

static int64_t falcon_getlength(BlockDriverState *bs)
{
    FalconClient *client = bs->opaque;
    return client->size;
}

static int64_t falcon_get_allocated_file_size(BlockDriverState *bs)
{
    return 0;
}

static void falcon_co_init_task(BlockDriverState *bs, FalconRPC *task)
{
    *task = (FalconRPC) {
        .co     = qemu_coroutine_self(),
        .bs     = bs,
    };
}

static void falcon_co_generic_bh_cb(int retval, void *opaque)
{
    FalconRPC *task = opaque;
    task->ret = retval;
    task->complete = 1;
    aio_co_wake(task->co);
}

static int coroutine_fn falcon_co_preadv(BlockDriverState *bs, uint64_t offset, uint64_t bytes, QEMUIOVector *iov, int flags)
{
    FalconClient *client = bs->opaque;
    FalconRPC task;
    falcon_co_init_task(bs, &task);
    task.iov = iov;

    qemu_mutex_lock(&client->mutex);
    falcon_proxy_rw(0, client->proxy, client->inode, offset, bytes, iov->iov, iov->niov, falcon_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

static int coroutine_fn falcon_co_pwritev(BlockDriverState *bs, uint64_t offset, uint64_t bytes, QEMUIOVector *iov, int flags)
{
    FalconClient *client = bs->opaque;
    FalconRPC task;
    falcon_co_init_task(bs, &task);
    task.iov = iov;

    qemu_mutex_lock(&client->mutex);
    falcon_proxy_rw(1, client->proxy, client->inode, offset, bytes, iov->iov, iov->niov, falcon_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

static int coroutine_fn falcon_co_flush(BlockDriverState *bs)
{
    FalconClient *client = bs->opaque;
    FalconRPC task;
    falcon_co_init_task(bs, &task);

    qemu_mutex_lock(&client->mutex);
    falcon_proxy_sync(client->proxy, falcon_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

static QemuOptsList falcon_create_opts = {
    .name = "falcon-create-opts",
    .head = QTAILQ_HEAD_INITIALIZER(falcon_create_opts.head),
    .desc = {
        {
            .name = BLOCK_OPT_SIZE,
            .type = QEMU_OPT_SIZE,
            .help = "Virtual disk size"
        },
        { /* end of list */ }
    }
};

static const char *falcon_strong_runtime_opts[] = {
    "image",
    "server.",

    NULL
};

static BlockDriver bdrv_falcon = {
    .format_name                    = "falcon",
    .protocol_name                  = "falcon",

    .instance_size                  = sizeof(FalconClient),
    .bdrv_parse_filename            = falcon_parse_filename,

    .bdrv_has_zero_init             = bdrv_has_zero_init_1,
    .bdrv_has_zero_init_truncate    = bdrv_has_zero_init_1,
    .bdrv_get_info                  = falcon_get_info,
    .bdrv_getlength                 = falcon_getlength,
    .bdrv_probe_blocksizes          = falcon_probe_blocksizes,

    // FIXME: Implement it along with per-inode statistics
    //.bdrv_get_allocated_file_size   = falcon_get_allocated_file_size,

    .bdrv_file_open                 = falcon_file_open,
    .bdrv_close                     = falcon_close,

    // Option list for the create operation
    .create_opts                    = &falcon_create_opts,

    // For qmp_blockdev_create(), used by the qemu monitor / QAPI
    // Requires patching QAPI IDL, thus unimplemented
    //.bdrv_co_create                 = falcon_co_create,

    // For bdrv_create(), used by qemu-img
    .bdrv_co_create_opts            = falcon_co_create_opts,

    .bdrv_co_truncate               = falcon_co_truncate,

    .bdrv_co_preadv                 = falcon_co_preadv,
    .bdrv_co_pwritev                = falcon_co_pwritev,
    .bdrv_co_flush_to_disk          = falcon_co_flush,

    .strong_runtime_opts            = falcon_strong_runtime_opts,
};

static void falcon_block_init(void)
{
    bdrv_register(&bdrv_falcon);
}

block_init(falcon_block_init);
