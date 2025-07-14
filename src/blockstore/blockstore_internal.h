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

#define COPY_BUF_JOURNAL 1
#define COPY_BUF_DATA 2
#define COPY_BUF_ZERO 4
#define COPY_BUF_CSUM_FILL 8
#define COPY_BUF_COALESCED 16
#define COPY_BUF_PADDED 32
#define COPY_BUF_SKIP_CSUM 64
