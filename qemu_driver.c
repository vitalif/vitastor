// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

// QEMU block driver

#define BUILD_DSO
#define _GNU_SOURCE
#include "qemu/osdep.h"
#include "block/block_int.h"
#include "qapi/error.h"
#include "qapi/qmp/qdict.h"
#include "qapi/qmp/qerror.h"
#include "qemu/uri.h"
#include "qemu/error-report.h"
#include "qemu/module.h"
#include "qemu/option.h"

#if QEMU_VERSION_MAJOR >= 3
#include "qemu/units.h"
#include "block/qdict.h"
#include "qemu/cutils.h"
#else
#include "qapi/qmp/qint.h"
#define qdict_put_int(options, name, num_val) qdict_put_obj(options, name, QOBJECT(qint_from_int(num_val)))
#define qdict_put_str(options, name, value) qdict_put_obj(options, name, QOBJECT(qstring_from_str(value)))
#define qobject_unref QDECREF
#endif

#include "qemu_proxy.h"

void qemu_module_dummy(void)
{
}

void DSO_STAMP_FUN(void)
{
}

typedef struct VitastorClient
{
    void *proxy;
    char *etcd_host;
    char *etcd_prefix;
    uint64_t inode;
    uint64_t pool;
    uint64_t size;
    int readonly;
    QemuMutex mutex;
} VitastorClient;

typedef struct VitastorRPC
{
    BlockDriverState *bs;
    Coroutine *co;
    QEMUIOVector *iov;
    int ret;
    int complete;
} VitastorRPC;

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

// vitastor[:key=value]*
// vitastor:etcd_host=127.0.0.1:inode=1:pool=1
static void vitastor_parse_filename(const char *filename, QDict *options, Error **errp)
{
    const char *start;
    char *p, *buf;

    if (!strstart(filename, "vitastor:", &start))
    {
        error_setg(errp, "File name must start with 'vitastor:'");
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
        if (!strcmp(name, "inode") || !strcmp(name, "pool") || !strcmp(name, "size"))
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
    if (!qdict_get_try_int(options, "inode", 0))
    {
        error_setg(errp, "inode is missing");
        goto out;
    }
    if (!(qdict_get_try_int(options, "inode", 0) >> (64-POOL_ID_BITS)) &&
        !qdict_get_try_int(options, "pool", 0))
    {
        error_setg(errp, "pool number is missing");
        goto out;
    }
    if (!qdict_get_try_int(options, "size", 0))
    {
        error_setg(errp, "size is missing");
        goto out;
    }
    if (!qdict_get_str(options, "etcd_host"))
    {
        error_setg(errp, "etcd_host is missing");
        goto out;
    }

out:
    g_free(buf);
    return;
}

static int vitastor_file_open(BlockDriverState *bs, QDict *options, int flags, Error **errp)
{
    VitastorClient *client = bs->opaque;
    int64_t ret = 0;
    client->etcd_host = g_strdup(qdict_get_try_str(options, "etcd_host"));
    client->etcd_prefix = g_strdup(qdict_get_try_str(options, "etcd_prefix"));
    client->inode = qdict_get_int(options, "inode");
    client->pool = qdict_get_int(options, "pool");
    if (client->pool)
        client->inode = (client->inode & ((1l << (64-POOL_ID_BITS)) - 1)) | (client->pool << (64-POOL_ID_BITS));
    client->size = qdict_get_int(options, "size");
    client->readonly = (flags & BDRV_O_RDWR) ? 1 : 0;
    client->proxy = vitastor_proxy_create(bdrv_get_aio_context(bs), client->etcd_host, client->etcd_prefix);
    //client->aio_context = bdrv_get_aio_context(bs);
    bs->total_sectors = client->size / BDRV_SECTOR_SIZE;
    qdict_del(options, "etcd_host");
    qdict_del(options, "etcd_prefix");
    qdict_del(options, "inode");
    qdict_del(options, "pool");
    qdict_del(options, "size");
    qemu_mutex_init(&client->mutex);
    return ret;
}

static void vitastor_close(BlockDriverState *bs)
{
    VitastorClient *client = bs->opaque;
    vitastor_proxy_destroy(client->proxy);
    qemu_mutex_destroy(&client->mutex);
    g_free(client->etcd_host);
    if (client->etcd_prefix)
        g_free(client->etcd_prefix);
}

#if QEMU_VERSION_MAJOR >= 3
static int vitastor_probe_blocksizes(BlockDriverState *bs, BlockSizes *bsz)
{
    bsz->phys = 4096;
    bsz->log = 4096;
    return 0;
}
#endif

static int coroutine_fn vitastor_co_create_opts(
#if QEMU_VERSION_MAJOR >= 4
    BlockDriver *drv,
#endif
    const char *url, QemuOpts *opts, Error **errp)
{
    QDict *options;
    int ret;

    options = qdict_new();
    vitastor_parse_filename(url, options, errp);
    if (*errp)
    {
        ret = -1;
        goto out;
    }

    // inodes don't require creation in Vitastor. FIXME: They will when there will be some metadata

    ret = 0;
out:
    qobject_unref(options);
    return ret;
}

#if QEMU_VERSION_MAJOR >= 3
static int coroutine_fn vitastor_co_truncate(BlockDriverState *bs, int64_t offset,
#if QEMU_VERSION_MAJOR >= 4
    bool exact,
#endif
    PreallocMode prealloc,
#if QEMU_VERSION_MAJOR >= 5 && QEMU_VERSION_MINOR >= 1 || QEMU_VERSION_MAJOR > 5
    BdrvRequestFlags flags,
#endif
    Error **errp)
{
    VitastorClient *client = bs->opaque;

    if (prealloc != PREALLOC_MODE_OFF)
    {
        error_setg(errp, "Unsupported preallocation mode '%s'", PreallocMode_str(prealloc));
        return -ENOTSUP;
    }

    // TODO: Resize inode to <offset> bytes
    client->size = offset / BDRV_SECTOR_SIZE;

    return 0;
}
#endif

static int vitastor_get_info(BlockDriverState *bs, BlockDriverInfo *bdi)
{
    bdi->cluster_size = 4096;
    return 0;
}

static int64_t vitastor_getlength(BlockDriverState *bs)
{
    VitastorClient *client = bs->opaque;
    return client->size;
}

#if QEMU_VERSION_MAJOR >= 3
static void vitastor_refresh_limits(BlockDriverState *bs, Error **errp)
#else
static int vitastor_refresh_limits(BlockDriverState *bs)
#endif
{
#if QEMU_VERSION_MAJOR >= 4
    bs->bl.request_alignment = 4096;
    bs->bl.min_mem_alignment = 4096;
#else
    bs->request_alignment = 4096;
#endif
    bs->bl.opt_mem_alignment = 4096;
#if QEMU_VERSION_MAJOR < 3
    return 0;
#endif
}

static int64_t vitastor_get_allocated_file_size(BlockDriverState *bs)
{
    return 0;
}

static void vitastor_co_init_task(BlockDriverState *bs, VitastorRPC *task)
{
    *task = (VitastorRPC) {
        .co     = qemu_coroutine_self(),
        .bs     = bs,
    };
}

static void vitastor_co_generic_bh_cb(int retval, void *opaque)
{
    VitastorRPC *task = opaque;
    task->ret = retval;
    task->complete = 1;
    if (qemu_coroutine_self() != task->co)
    {
#if QEMU_VERSION_MAJOR >= 3
        aio_co_wake(task->co);
#else
        qemu_coroutine_enter(task->co, NULL);
        qemu_aio_release(task);
#endif
    }
}

static int coroutine_fn vitastor_co_preadv(BlockDriverState *bs, uint64_t offset, uint64_t bytes, QEMUIOVector *iov, int flags)
{
    VitastorClient *client = bs->opaque;
    VitastorRPC task;
    vitastor_co_init_task(bs, &task);
    task.iov = iov;

    qemu_mutex_lock(&client->mutex);
    vitastor_proxy_rw(0, client->proxy, client->inode, offset, bytes, iov->iov, iov->niov, vitastor_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

static int coroutine_fn vitastor_co_pwritev(BlockDriverState *bs, uint64_t offset, uint64_t bytes, QEMUIOVector *iov, int flags)
{
    VitastorClient *client = bs->opaque;
    VitastorRPC task;
    vitastor_co_init_task(bs, &task);
    task.iov = iov;

    qemu_mutex_lock(&client->mutex);
    vitastor_proxy_rw(1, client->proxy, client->inode, offset, bytes, iov->iov, iov->niov, vitastor_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

#if QEMU_VERSION_MAJOR < 3
static int coroutine_fn vitastor_co_readv(BlockDriverState *bs, int64_t sector_num, int nb_sectors, QEMUIOVector *iov)
{
    return vitastor_co_preadv(bs, sector_num*BDRV_SECTOR_SIZE, nb_sectors*BDRV_SECTOR_SIZE, iov, 0);
}

static int coroutine_fn vitastor_co_writev(BlockDriverState *bs, int64_t sector_num, int nb_sectors, QEMUIOVector *iov)
{
    return vitastor_co_pwritev(bs, sector_num*BDRV_SECTOR_SIZE, nb_sectors*BDRV_SECTOR_SIZE, iov, 0);
}
#endif

static int coroutine_fn vitastor_co_flush(BlockDriverState *bs)
{
    VitastorClient *client = bs->opaque;
    VitastorRPC task;
    vitastor_co_init_task(bs, &task);

    qemu_mutex_lock(&client->mutex);
    vitastor_proxy_sync(client->proxy, vitastor_co_generic_bh_cb, &task);
    qemu_mutex_unlock(&client->mutex);

    while (!task.complete)
    {
        qemu_coroutine_yield();
    }

    return task.ret;
}

#if QEMU_VERSION_MAJOR >= 3
static QemuOptsList vitastor_create_opts = {
    .name = "vitastor-create-opts",
    .head = QTAILQ_HEAD_INITIALIZER(vitastor_create_opts.head),
    .desc = {
        {
            .name = BLOCK_OPT_SIZE,
            .type = QEMU_OPT_SIZE,
            .help = "Virtual disk size"
        },
        { /* end of list */ }
    }
};
#else
static QEMUOptionParameter vitastor_create_opts[] = {
    {
        .name = BLOCK_OPT_SIZE,
        .type = OPT_SIZE,
        .help = "Virtual disk size"
    },
    { NULL }
};
#endif

static const char *vitastor_strong_runtime_opts[] = {
    "inode",
    "pool",
    "etcd_host",
    "etcd_prefix",

    NULL
};

static BlockDriver bdrv_vitastor = {
    .format_name                    = "vitastor",
    .protocol_name                  = "vitastor",

    .instance_size                  = sizeof(VitastorClient),
    .bdrv_parse_filename            = vitastor_parse_filename,

    .bdrv_has_zero_init             = bdrv_has_zero_init_1,
    .bdrv_get_info                  = vitastor_get_info,
    .bdrv_getlength                 = vitastor_getlength,
#if QEMU_VERSION_MAJOR >= 3
    .bdrv_probe_blocksizes          = vitastor_probe_blocksizes,
#endif
    .bdrv_refresh_limits            = vitastor_refresh_limits,

    // FIXME: Implement it along with per-inode statistics
    //.bdrv_get_allocated_file_size   = vitastor_get_allocated_file_size,

    .bdrv_file_open                 = vitastor_file_open,
    .bdrv_close                     = vitastor_close,

    // Option list for the create operation
#if QEMU_VERSION_MAJOR >= 3
    .create_opts                    = &vitastor_create_opts,
#else
    .create_options                 = vitastor_create_opts,
#endif

    // For qmp_blockdev_create(), used by the qemu monitor / QAPI
    // Requires patching QAPI IDL, thus unimplemented
    //.bdrv_co_create                 = vitastor_co_create,

#if QEMU_VERSION_MAJOR >= 3
    // For bdrv_create(), used by qemu-img
    .bdrv_co_create_opts            = vitastor_co_create_opts,

    .bdrv_co_truncate               = vitastor_co_truncate,

    .bdrv_co_preadv                 = vitastor_co_preadv,
    .bdrv_co_pwritev                = vitastor_co_pwritev,
#else
    .bdrv_co_readv                  = vitastor_co_readv,
    .bdrv_co_writev                 = vitastor_co_writev,
#endif

    .bdrv_co_flush_to_disk          = vitastor_co_flush,

#if QEMU_VERSION_MAJOR >= 4
    .strong_runtime_opts            = vitastor_strong_runtime_opts,
#endif
};

static void vitastor_block_init(void)
{
    bdrv_register(&bdrv_vitastor);
}

block_init(vitastor_block_init);
