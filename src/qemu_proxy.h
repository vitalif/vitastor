// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

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

// Our exports
typedef void VitastorIOHandler(int retval, void *opaque);
void* vitastor_proxy_create(AioContext *ctx, const char *etcd_host, const char *etcd_prefix);
void vitastor_proxy_destroy(void *client);
void vitastor_proxy_rw(int write, void *client, uint64_t inode, uint64_t offset, uint64_t len,
    struct iovec *iov, int iovcnt, VitastorIOHandler cb, void *opaque);
void vitastor_proxy_sync(void *client, VitastorIOHandler cb, void *opaque);

#ifdef __cplusplus
}
#endif

#endif
