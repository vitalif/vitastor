// C-C++ proxy for the QEMU driver
// (QEMU headers don't compile with g++)

#include <sys/epoll.h>

#include "cluster_client.h"
#include "qemu_proxy.h"

extern "C"
{
    // QEMU
    typedef void IOHandler(void *opaque);
    void qemu_set_fd_handler(int fd, IOHandler *fd_read, IOHandler *fd_write, void *opaque);
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

    QemuProxy(const char *etcd_host, const char *etcd_prefix)
    {
        json11::Json cfg = json11::Json::object {
            { "etcd_address", std::string(etcd_host) },
            { "etcd_prefix", std::string(etcd_prefix ? etcd_prefix : "/microceph") },
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
            qemu_set_fd_handler(fd, &QemuProxy::read_handler, wr ? &QemuProxy::write_handler : NULL, &handlers[fd]);
        }
        else
        {
            handlers.erase(fd);
            qemu_set_fd_handler(fd, NULL, NULL, NULL);
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

void* falcon_proxy_create(const char *etcd_host, const char *etcd_prefix)
{
    QemuProxy *p = new QemuProxy(etcd_host, etcd_prefix);
    return p;
}

void falcon_proxy_destroy(void *client)
{
    QemuProxy *p = (QemuProxy*)client;
    delete p;
}

void falcon_proxy_rw(int write, void *client, uint64_t inode, uint64_t offset, uint64_t len,
    iovec *iov, int iovcnt, FalconIOHandler cb, void *opaque)
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

void falcon_proxy_sync(void *client, FalconIOHandler cb, void *opaque)
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

}
