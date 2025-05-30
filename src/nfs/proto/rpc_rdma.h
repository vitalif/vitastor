/*
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#ifndef _RPC_RDMA_H_RPCGEN
#define _RPC_RDMA_H_RPCGEN

#include "xdr_impl.h"


#ifdef __cplusplus
extern "C" {
#endif


struct xdr_rdma_segment {
	uint32_t handle;
	uint32_t length;
	uint64_t offset;
};
typedef struct xdr_rdma_segment xdr_rdma_segment;

struct xdr_read_chunk {
	uint32_t position;
	struct xdr_rdma_segment target;
};
typedef struct xdr_read_chunk xdr_read_chunk;

struct xdr_read_list {
	struct xdr_read_chunk entry;
	struct xdr_read_list *next;
};
typedef struct xdr_read_list xdr_read_list;

struct xdr_write_chunk {
	struct {
		u_int target_len;
		struct xdr_rdma_segment *target_val;
	} target;
};
typedef struct xdr_write_chunk xdr_write_chunk;

struct xdr_write_list {
	struct xdr_write_chunk entry;
	struct xdr_write_list *next;
};
typedef struct xdr_write_list xdr_write_list;

struct rpc_rdma_header {
	struct xdr_read_list *rdma_reads;
	struct xdr_write_list *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
};
typedef struct rpc_rdma_header rpc_rdma_header;

struct rpc_rdma_header_nomsg {
	struct xdr_read_list *rdma_reads;
	struct xdr_write_list *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
};
typedef struct rpc_rdma_header_nomsg rpc_rdma_header_nomsg;

struct rpc_rdma_header_padded {
	uint32_t rdma_align;
	uint32_t rdma_thresh;
	struct xdr_read_list *rdma_reads;
	struct xdr_write_list *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
};
typedef struct rpc_rdma_header_padded rpc_rdma_header_padded;

enum rpc_rdma_errcode {
	ERR_VERS = 1,
	ERR_CHUNK = 2,
};
typedef enum rpc_rdma_errcode rpc_rdma_errcode;

struct rpc_rdma_errvers {
	uint32_t rdma_vers_low;
	uint32_t rdma_vers_high;
};
typedef struct rpc_rdma_errvers rpc_rdma_errvers;

struct rpc_rdma_error {
	rpc_rdma_errcode err;
	union {
		rpc_rdma_errvers range;
	};
};
typedef struct rpc_rdma_error rpc_rdma_error;

enum rdma_proc {
	RDMA_MSG = 0,
	RDMA_NOMSG = 1,
	RDMA_MSGP = 2,
	RDMA_DONE = 3,
	RDMA_ERROR = 4,
};
typedef enum rdma_proc rdma_proc;

struct rdma_body {
	rdma_proc proc;
	union {
		rpc_rdma_header rdma_msg;
		rpc_rdma_header_nomsg rdma_nomsg;
		rpc_rdma_header_padded rdma_msgp;
		rpc_rdma_error rdma_error;
	};
};
typedef struct rdma_body rdma_body;

struct rdma_msg {
	uint32_t rdma_xid;
	uint32_t rdma_vers;
	uint32_t rdma_credit;
	rdma_body rdma_body;
};
typedef struct rdma_msg rdma_msg;

/* the xdr functions */


extern  bool_t xdr_xdr_rdma_segment (XDR *, xdr_rdma_segment*);
extern  bool_t xdr_xdr_read_chunk (XDR *, xdr_read_chunk*);
extern  bool_t xdr_xdr_read_list (XDR *, xdr_read_list*);
extern  bool_t xdr_xdr_write_chunk (XDR *, xdr_write_chunk*);
extern  bool_t xdr_xdr_write_list (XDR *, xdr_write_list*);
extern  bool_t xdr_rpc_rdma_header (XDR *, rpc_rdma_header*);
extern  bool_t xdr_rpc_rdma_header_nomsg (XDR *, rpc_rdma_header_nomsg*);
extern  bool_t xdr_rpc_rdma_header_padded (XDR *, rpc_rdma_header_padded*);
extern  bool_t xdr_rpc_rdma_errcode (XDR *, rpc_rdma_errcode*);
extern  bool_t xdr_rpc_rdma_errvers (XDR *, rpc_rdma_errvers*);
extern  bool_t xdr_rpc_rdma_error (XDR *, rpc_rdma_error*);
extern  bool_t xdr_rdma_proc (XDR *, rdma_proc*);
extern  bool_t xdr_rdma_body (XDR *, rdma_body*);
extern  bool_t xdr_rdma_msg (XDR *, rdma_msg*);


#ifdef __cplusplus
}
#endif

#endif /* !_RPC_RDMA_H_RPCGEN */
