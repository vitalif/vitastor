// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

// Simplified C client library for QEMU, fio and other external drivers
// Also acts as a C-C++ proxy for the QEMU driver (QEMU headers don't compile with g++)

#include <sys/epoll.h>

#include "ringloop.h"
#include "epoll_manager.h"
#include "cluster_client.h"

#include "vitastor_c.h"

struct vitastor_qemu_fd_t
{
    int fd;
    std::function<void(int, int)> callback;
};

struct vitastor_c
{
    std::map<int, vitastor_qemu_fd_t> handlers;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    timerfd_manager_t *tfd = NULL;
    cluster_client_t *cli = NULL;

    QEMUSetFDHandler *aio_set_fd_handler = NULL;
    void *aio_ctx = NULL;
};

extern "C" {

static json11::Json vitastor_c_common_config(const char *config_path, const char *etcd_host, const char *etcd_prefix,
    int use_rdma, const char *rdma_device, int rdma_port_num, int rdma_gid_index, int rdma_mtu, int log_level)
{
    json11::Json::object cfg;
    if (config_path)
        cfg["config_path"] = std::string(config_path);
    if (etcd_host)
        cfg["etcd_address"] = std::string(etcd_host);
    if (etcd_prefix)
        cfg["etcd_prefix"] = std::string(etcd_prefix);
    // -1 means unspecified
    if (use_rdma >= 0)
        cfg["use_rdma"] = use_rdma > 0;
    if (rdma_device)
        cfg["rdma_device"] = std::string(rdma_device);
    if (rdma_port_num)
        cfg["rdma_port_num"] = rdma_port_num;
    if (rdma_gid_index)
        cfg["rdma_gid_index"] = rdma_gid_index;
    if (rdma_mtu)
        cfg["rdma_mtu"] = rdma_mtu;
    if (log_level)
        cfg["log_level"] = log_level;
    return json11::Json(cfg);
}

static void vitastor_c_read_handler(void *opaque)
{
    vitastor_qemu_fd_t *data = (vitastor_qemu_fd_t *)opaque;
    data->callback(data->fd, EPOLLIN);
}

static void vitastor_c_write_handler(void *opaque)
{
    vitastor_qemu_fd_t *data = (vitastor_qemu_fd_t *)opaque;
    data->callback(data->fd, EPOLLOUT);
}

vitastor_c *vitastor_c_create_qemu(QEMUSetFDHandler *aio_set_fd_handler, void *aio_context,
    const char *config_path, const char *etcd_host, const char *etcd_prefix,
    int use_rdma, const char *rdma_device, int rdma_port_num, int rdma_gid_index, int rdma_mtu, int log_level)
{
    json11::Json cfg_json = vitastor_c_common_config(
        config_path, etcd_host, etcd_prefix, use_rdma,
        rdma_device, rdma_port_num, rdma_gid_index, rdma_mtu, log_level
    );
    vitastor_c *self = new vitastor_c;
    self->aio_set_fd_handler = aio_set_fd_handler;
    self->aio_ctx = aio_context;
    self->tfd = new timerfd_manager_t([self](int fd, bool wr, std::function<void(int, int)> callback)
    {
        if (callback != NULL)
        {
            self->handlers[fd] = { .fd = fd, .callback = callback };
            self->aio_set_fd_handler(self->aio_ctx, fd, false,
                vitastor_c_read_handler, wr ? vitastor_c_write_handler : NULL, NULL, &self->handlers[fd]);
        }
        else
        {
            self->handlers.erase(fd);
            self->aio_set_fd_handler(self->aio_ctx, fd, false, NULL, NULL, NULL, NULL);
        }
    });
    self->cli = new cluster_client_t(NULL, self->tfd, cfg_json);
    return self;
}

vitastor_c *vitastor_c_create_uring(const char *config_path, const char *etcd_host, const char *etcd_prefix,
    int use_rdma, const char *rdma_device, int rdma_port_num, int rdma_gid_index, int rdma_mtu, int log_level)
{
    json11::Json cfg_json = vitastor_c_common_config(
        config_path, etcd_host, etcd_prefix, use_rdma,
        rdma_device, rdma_port_num, rdma_gid_index, rdma_mtu, log_level
    );
    vitastor_c *self = new vitastor_c;
    self->ringloop = new ring_loop_t(512);
    self->epmgr = new epoll_manager_t(self->ringloop);
    self->cli = new cluster_client_t(self->ringloop, self->epmgr->tfd, cfg_json);
    return self;
}

vitastor_c *vitastor_c_create_uring_json(const char **options, int options_len)
{
    json11::Json::object cfg;
    for (int i = 0; i < options_len-1; i += 2)
    {
        cfg[options[i]] = std::string(options[i+1]);
    }
    json11::Json cfg_json(cfg);
    vitastor_c *self = new vitastor_c;
    self->ringloop = new ring_loop_t(512);
    self->epmgr = new epoll_manager_t(self->ringloop);
    self->cli = new cluster_client_t(self->ringloop, self->epmgr->tfd, cfg_json);
    return self;
}

void vitastor_c_destroy(vitastor_c *client)
{
    delete client->cli;
    if (client->epmgr)
        delete client->epmgr;
    else
        delete client->tfd;
    if (client->ringloop)
        delete client->ringloop;
    delete client;
}

int vitastor_c_is_ready(vitastor_c *client)
{
    return client->cli->is_ready();
}

void vitastor_c_uring_wait_ready(vitastor_c *client)
{
    while (!client->cli->is_ready())
    {
        client->ringloop->loop();
        if (client->cli->is_ready())
            break;
        client->ringloop->wait();
    }
}

void vitastor_c_uring_handle_events(vitastor_c *client)
{
    client->ringloop->loop();
}

void vitastor_c_uring_wait_events(vitastor_c *client)
{
    client->ringloop->wait();
}

void vitastor_c_read(vitastor_c *client, uint64_t inode, uint64_t offset, uint64_t len,
    struct iovec *iov, int iovcnt, VitastorReadHandler cb, void *opaque)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = inode;
    op->offset = offset;
    op->len = len;
    for (int i = 0; i < iovcnt; i++)
    {
        op->iov.push_back(iov[i].iov_base, iov[i].iov_len);
    }
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        cb(opaque, op->retval, op->version);
        delete op;
    };
    client->cli->execute(op);
}

void vitastor_c_write(vitastor_c *client, uint64_t inode, uint64_t offset, uint64_t len, uint64_t check_version,
    struct iovec *iov, int iovcnt, VitastorIOHandler cb, void *opaque)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = inode;
    op->offset = offset;
    op->len = len;
    op->version = check_version;
    for (int i = 0; i < iovcnt; i++)
    {
        op->iov.push_back(iov[i].iov_base, iov[i].iov_len);
    }
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        cb(opaque, op->retval);
        delete op;
    };
    client->cli->execute(op);
}

void vitastor_c_read_bitmap(vitastor_c *client, uint64_t inode, uint64_t offset, uint64_t len,
    int with_parents, VitastorReadBitmapHandler cb, void *opaque)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = with_parents ? OSD_OP_READ_CHAIN_BITMAP : OSD_OP_READ_BITMAP;
    op->inode = inode;
    op->offset = offset;
    op->len = len;
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        uint8_t *bitmap = NULL;
        if (op->retval >= 0)
        {
            bitmap = (uint8_t*)op->bitmap_buf;
            op->bitmap_buf = NULL;
        }
        cb(opaque, op->retval, bitmap);
        delete op;
    };
    client->cli->execute(op);
}

void vitastor_c_sync(vitastor_c *client, VitastorIOHandler cb, void *opaque)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_SYNC;
    op->callback = [cb, opaque](cluster_op_t *op)
    {
        cb(opaque, op->retval);
        delete op;
    };
    client->cli->execute(op);
}

void vitastor_c_watch_inode(vitastor_c *client, char *image, VitastorIOHandler cb, void *opaque)
{
    client->cli->on_ready([=]()
    {
        auto watch = client->cli->st_cli.watch_inode(std::string(image));
        cb(opaque, (long)watch);
    });
}

void vitastor_c_close_watch(vitastor_c *client, void *handle)
{
    client->cli->st_cli.close_watch((inode_watch_t*)handle);
}

uint64_t vitastor_c_inode_get_size(void *handle)
{
    inode_watch_t *watch = (inode_watch_t*)handle;
    return watch->cfg.size;
}

uint64_t vitastor_c_inode_get_num(void *handle)
{
    inode_watch_t *watch = (inode_watch_t*)handle;
    return watch->cfg.num;
}

uint32_t vitastor_c_inode_get_block_size(vitastor_c *client, uint64_t inode_num)
{
    auto pool_it = client->cli->st_cli.pool_config.find(INODE_POOL(inode_num));
    if (pool_it == client->cli->st_cli.pool_config.end())
        return 0;
    auto & pool_cfg = pool_it->second;
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    return pool_cfg.data_block_size * pg_data_size;
}

uint32_t vitastor_c_inode_get_bitmap_granularity(vitastor_c *client, uint64_t inode_num)
{
    auto pool_it = client->cli->st_cli.pool_config.find(INODE_POOL(inode_num));
    if (pool_it == client->cli->st_cli.pool_config.end())
        return 0;
    // FIXME: READ_BITMAP may fails if parent bitmap granularity differs from inode bitmap granularity
    return pool_it->second.bitmap_granularity;
}

int vitastor_c_inode_get_readonly(void *handle)
{
    inode_watch_t *watch = (inode_watch_t*)handle;
    return watch->cfg.readonly;
}

}
