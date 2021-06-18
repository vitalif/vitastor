// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

// Simplified C client library for QEMU, fio and other external drivers

#ifndef VITASTOR_QEMU_PROXY_H
#define VITASTOR_QEMU_PROXY_H

#ifndef POOL_ID_BITS
#define POOL_ID_BITS 16
#endif
#include <stdint.h>
#include <sys/uio.h>

#ifdef __cplusplus
extern "C" {
#endif

struct vitastor_c;
typedef struct vitastor_c vitastor_c;

typedef void VitastorIOHandler(long retval, void *opaque);

// QEMU
typedef void IOHandler(void *opaque);
typedef void QEMUSetFDHandler(void *ctx, int fd, int is_external, IOHandler *fd_read, IOHandler *fd_write, void *poll_fn, void *opaque);

vitastor_c *vitastor_c_create_qemu(QEMUSetFDHandler *aio_set_fd_handler, void *aio_context,
    const char *config_path, const char *etcd_host, const char *etcd_prefix,
    bool use_rdma, const char *rdma_device, int rdma_port_num, int rdma_gid_index, int rdma_mtu, int log_level);
vitastor_c *vitastor_c_create_uring(const char *config_path, const char *etcd_host, const char *etcd_prefix,
    int use_rdma, const char *rdma_device, int rdma_port_num, int rdma_gid_index, int rdma_mtu, int log_level);
vitastor_c *vitastor_c_create_uring_json(const char **options, int options_len);
void vitastor_c_destroy(vitastor_c *client);
int vitastor_c_is_ready(vitastor_c *client);
void vitastor_c_uring_wait_ready(vitastor_c *client);
void vitastor_c_uring_handle_events(vitastor_c *client);
void vitastor_c_uring_wait_events(vitastor_c *client);
void vitastor_c_read(vitastor_c *client, uint64_t inode, uint64_t offset, uint64_t len,
    struct iovec *iov, int iovcnt, VitastorIOHandler cb, void *opaque);
void vitastor_c_write(vitastor_c *client, uint64_t inode, uint64_t offset, uint64_t len,
    struct iovec *iov, int iovcnt, VitastorIOHandler cb, void *opaque);
void vitastor_c_sync(vitastor_c *client, VitastorIOHandler cb, void *opaque);
void vitastor_c_watch_inode(vitastor_c *client, char *image, VitastorIOHandler cb, void *opaque);
void vitastor_c_close_watch(vitastor_c *client, void *handle);
uint64_t vitastor_c_inode_get_size(void *handle);
uint64_t vitastor_c_inode_get_num(void *handle);
int vitastor_c_inode_get_readonly(void *handle);

#ifdef __cplusplus
}
#endif

#endif
