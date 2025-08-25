/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_COMPAT_H
#define LIBURING_COMPAT_H

#include <linux/version.h>

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5,1,0)
#include <linux/time_types.h>
#else
struct __kernel_timespec {
    int64_t tv_sec;
    int64_t tv_nsec;
};
#endif

/* <linux/time_types.h> is included above and not needed again */
#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5,6,0)
#include <linux/openat2.h>
#else
struct open_how {
    uint64_t flags;
    uint16_t mode;
    uint16_t __padding[3];
    uint64_t resolve;
};
#endif

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,14,0)
typedef int __kernel_rwf_t;
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,12,0)
#include <linux/blkdev.h>
#else
#include <linux/ioctl.h>
#ifndef BLOCK_URING_CMD_DISCARD
#define BLOCK_URING_CMD_DISCARD _IO(0x12, 0)
#endif
#endif

#endif
