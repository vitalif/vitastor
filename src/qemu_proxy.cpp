// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

// C-C++ proxy for the QEMU driver
// (QEMU headers don't compile with g++)

#include <sys/epoll.h>

#include "cluster_client.h"

typedef void* AioContext;
#include "qemu_proxy.h"

extern "C"
{
    // QEMU
    typedef void IOHandler(void *opaque);
    void aio_set_fd_handler(AioContext *ctx, int fd, int is_external, IOHandler *fd_read, IOHandler *fd_write, void *poll_fn, void *opaque);
}

struct QemuProxyData
{
    int fd;
    std::function<void(int, int)> callback;
};

class QemuProxy
{
    std::map<int, QemuProxyData> handlers;

public:

    timerfd_manager_t *tfd;
    cluster_client_t *cli;
    AioContext *ctx;

    QemuProxy(AioContext *ctx, const char *etcd_host, const char *etcd_prefix)
    {
        this->ctx = ctx;
        json11::Json cfg = json11::Json::object {
            { "etcd_address", std::string(etcd_host) },
            { "etcd_prefix", std::string(etcd_prefix ? etcd_prefix : "/vitastor") },
        };
        tfd = new timerfd_manager_t([this](int fd, bool wr, std::function<void(int, int)> callback) { set_fd_handler(fd, wr, callback); });
        cli = new cluster_client_t(NULL, tfd, cfg);
    }

    ~QemuProxy()
    {
        delete cli;
        delete tfd;
    }

    void set_fd_handler(int fd, bool wr, std::function<void(int, int)> callback)
    {
        if (callback != NULL)
        {
            handlers[fd] = { .fd = fd, .callback = callback };
            aio_set_fd_handler(ctx, fd, false, &QemuProxy::read_handler, wr ? &QemuProxy::write_handler : NULL, NULL, &handlers[fd]);
        }
        else
        {
            handlers.erase(fd);
            aio_set_fd_handler(ctx, fd, false, NULL, NULL, NULL, NULL);
        }
    }

    static void read_handler(void *opaque)
    {
        QemuProxyData *data = (QemuProxyData *)opaque;
        data->callback(data->fd, EPOLLIN);
    }

    static void write_handler(void *opaque)
    {
        QemuProxyData *data = (QemuProxyData *)opaque;
        data->callback(data->fd, EPOLLOUT);
    }
};

extern "C" {

void* vitastor_proxy_create(AioContext *ctx, const char *etcd_host, const char *etcd_prefix)
{
    QemuProxy *p = new QemuProxy(ctx, etcd_host, etcd_prefix);
    return p;
}

void vitastor_proxy_destroy(void *client)
{
    QemuProxy *p = (QemuProxy*)client;
    delete p;
}

void vitastor_proxy_rw(int write, void *client, uint64_t inode, uint64_t offset, uint64_t len,
    iovec *iov, int iovcnt, VitastorIOHandler cb, void *opaque)
{
    QemuProxy *p = (QemuProxy*)client;
    cluster_op_t *op = new cluster_op_t;
    op->opcode = write ? OSD_OP_WRITE : OSD_OP_READ;
    op->inode = inode;
    op->offset = offset;
    op->len = len;
    for (int i = 0; i < iovcnt; i++)
    {
        op->iov.push_back(iov[i].iov_base, iov[i].iov_len);
    }
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        cb(op->retval, opaque);
        delete op;
    };
    p->cli->execute(op);
}

void vitastor_proxy_sync(void *client, VitastorIOHandler cb, void *opaque)
{
    QemuProxy *p = (QemuProxy*)client;
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_SYNC;
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        cb(op->retval, opaque);
        delete op;
    };
    p->cli->execute(op);
}

void vitastor_proxy_watch_metadata(void *client, char *image, VitastorIOHandler cb, void *opaque)
{
    QemuProxy *p = (QemuProxy*)client;
    p->cli->on_ready([&]()
    {
        auto watch = p->cli->st_cli.watch_inode(std::string(image));
        cb((long)watch, opaque);
    });
}

void vitastor_proxy_close_watch(void *client, void *watch)
{
    QemuProxy *p = (QemuProxy*)client;
    p->cli->st_cli.close_watch((inode_watch_t*)watch);
}

uint64_t vitastor_proxy_get_size(void *watch_ptr)
{
    inode_watch_t *watch = (inode_watch_t*)watch_ptr;
    return watch->cfg.size;
}

uint64_t vitastor_proxy_get_inode_num(void *watch_ptr)
{
    inode_watch_t *watch = (inode_watch_t*)watch_ptr;
    return watch->cfg.num;
}

int vitastor_proxy_get_readonly(void *watch_ptr)
{
    inode_watch_t *watch = (inode_watch_t*)watch_ptr;
    return watch->cfg.readonly;
}

}
