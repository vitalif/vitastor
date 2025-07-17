#pragma once

// States are not stored on disk. Instead, they're deduced from the journal

#define BS_ST_SMALL_WRITE 0x01
#define BS_ST_BIG_WRITE 0x02
#define BS_ST_DELETE 0x03

#define BS_ST_WAIT_DEL 0x10
#define BS_ST_WAIT_BIG 0x20
#define BS_ST_IN_FLIGHT 0x30
#define BS_ST_SUBMITTED 0x40
#define BS_ST_WRITTEN 0x50
#define BS_ST_SYNCED 0x60
#define BS_ST_STABLE 0x70

#define BS_ST_INSTANT 0x100

#define BS_ST_TYPE_MASK 0x0F
#define BS_ST_WORKFLOW_MASK 0xF0
#define IS_IN_FLIGHT(st) (((st) & 0xF0) <= BS_ST_SUBMITTED)
#define IS_STABLE(st) (((st) & 0xF0) == BS_ST_STABLE)
#define IS_SYNCED(st) (((st) & 0xF0) >= BS_ST_SYNCED)
#define IS_JOURNAL(st) (((st) & 0x0F) == BS_ST_SMALL_WRITE)
#define IS_BIG_WRITE(st) (((st) & 0x0F) == BS_ST_BIG_WRITE)
#define IS_DELETE(st) (((st) & 0x0F) == BS_ST_DELETE)
#define IS_INSTANT(st) (((st) & BS_ST_TYPE_MASK) == BS_ST_DELETE || ((st) & BS_ST_INSTANT))

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
#define WAIT_JOURNAL 3
// Suspend operation until the next journal sector buffer is free
#define WAIT_JOURNAL_BUFFER 4
// Suspend operation until there is some free space on the data device
#define WAIT_FREE 5

#define COPY_BUF_JOURNAL 1
#define COPY_BUF_DATA 2
#define COPY_BUF_ZERO 4
#define COPY_BUF_CSUM_FILL 8
#define COPY_BUF_COALESCED 16
#define COPY_BUF_META_BLOCK 32
#define COPY_BUF_JOURNALED_BIG 64

#define STAB_SPLIT_DONE 1
#define STAB_SPLIT_WAIT 2
#define STAB_SPLIT_SYNC 3
#define STAB_SPLIT_TODO 4
