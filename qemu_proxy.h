#ifndef FALCON_QEMU_PROXY_H
#define FALCON_QEMU_PROXY_H

#include <stdint.h>
#include <sys/uio.h>

#ifdef __cplusplus
extern "C" {
#endif

// Our exports
typedef void FalconIOHandler(int retval, void *opaque);
void* falcon_proxy_create(const char *etcd_host, const char *etcd_prefix);
void falcon_proxy_destroy(void *client);
void falcon_proxy_rw(int write, void *client, uint64_t inode, uint64_t offset, uint64_t len,
    struct iovec *iov, int iovcnt, FalconIOHandler cb, void *opaque);
void falcon_proxy_sync(void *client, FalconIOHandler cb, void *opaque);

#ifdef __cplusplus
}
#endif

#endif
