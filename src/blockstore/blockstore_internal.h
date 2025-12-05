// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#define BS_SUBMIT_CHECK_SQES(n) \
    if (ringloop->space_left() < (n))\
    {\
        /* Pause until there are more requests available */\
        PRIV(op)->wait_detail = (n);\
        PRIV(op)->wait_for = WAIT_SQE;\
        return 0;\
    }

#define BS_SUBMIT_GET_SQE(sqe, data) \
    BS_SUBMIT_GET_ONLY_SQE(sqe); \
    struct ring_data_t *data = ((ring_data_t*)sqe->user_data)

#define BS_SUBMIT_GET_ONLY_SQE(sqe) \
    struct io_uring_sqe *sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        PRIV(op)->wait_detail = 1;\
        PRIV(op)->wait_for = WAIT_SQE;\
        return 0;\
    }

#define BS_SUBMIT_GET_SQE_DECL(sqe) \
    sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        PRIV(op)->wait_detail = 1;\
        PRIV(op)->wait_for = WAIT_SQE;\
        return 0;\
    }

#define PRIV(op) ((blockstore_op_private_t*)(op)->private_data)
#define FINISH_OP(op) PRIV(op)->~blockstore_op_private_t(); std::function<void (blockstore_op_t*)>(op->callback)(op)

// Suspend operation until there are more free SQEs
#define WAIT_SQE 1
// Suspend operation until there are <wait_detail> bytes of free space in the journal on disk
#define WAIT_COMPACTION 2

#define COPY_BUF_JOURNAL    0x01
#define COPY_BUF_DATA       0x02
#define COPY_BUF_ZERO       0x04
#define COPY_BUF_CSUM_FILL  0x08
#define COPY_BUF_COALESCED  0x10
#define COPY_BUF_PADDED     0x20
#define COPY_BUF_SKIP_CSUM  0x40

#ifndef RWF_ATOMIC
#define RWF_ATOMIC 0x40
#endif

#ifndef RWF_DSYNC
#define RWF_DSYNC 0x02
#endif
