/* RFC 8166 - Remote Direct Memory Access Transport for Remote Procedure Call Version 1 */

/*
 * Copyright (c) 2010-2017 IETF Trust and the persons
 * identified as authors of the code.  All rights reserved.
 *
 * The authors of the code are:
 * B. Callaghan, T. Talpey, and C. Lever
 *
 * Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the
 * following conditions are met:
 *
 * - Redistributions of source code must retain the above
 *   copyright notice, this list of conditions and the
 *   following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the
 *   following disclaimer in the documentation and/or other
 *   materials provided with the distribution.
 *
 * - Neither the name of Internet Society, IETF or IETF
 *   Trust, nor the names of specific contributors, may be
 *   used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *   AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *   WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *   FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 *   EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *   LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *   NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 *   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *   IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *   ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Plain RDMA segment (Section 3.4.3)
 */
struct xdr_rdma_segment {
	uint32_t handle;           /* Registered memory handle */
	uint32_t length;           /* Length of the chunk in bytes */
	uint64_t offset;           /* Chunk virtual address or offset */
};

/*
 * RDMA read segment (Section 3.4.5)
 */
struct xdr_read_chunk {
	uint32_t position;        /* Position in XDR stream */
	struct xdr_rdma_segment target;
};

/*
 * Read list (Section 4.3.1)
 */
struct xdr_read_list {
	struct xdr_read_chunk entry;
	struct xdr_read_list  *next;
};

/*
 * Write chunk (Section 3.4.6)
 */
struct xdr_write_chunk {
	struct xdr_rdma_segment target<>;
};

/*
 * Write list (Section 4.3.2)
 */
struct xdr_write_list {
	struct xdr_write_chunk entry;
	struct xdr_write_list  *next;
};

/*
 * Chunk lists (Section 4.3)
 */
struct rpc_rdma_header {
	struct xdr_read_list   *rdma_reads;
	struct xdr_write_list  *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
	/* rpc body follows */
};

struct rpc_rdma_header_nomsg {
	struct xdr_read_list   *rdma_reads;
	struct xdr_write_list  *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
};

/* Not to be used */
struct rpc_rdma_header_padded {
	uint32_t               rdma_align;
	uint32_t               rdma_thresh;
	struct xdr_read_list   *rdma_reads;
	struct xdr_write_list  *rdma_writes;
	struct xdr_write_chunk *rdma_reply;
	/* rpc body follows */
};

/*
 * Error handling (Section 4.5)
 */
enum rpc_rdma_errcode {
	ERR_VERS = 1,       /* Value fixed for all versions */
	ERR_CHUNK = 2
};

/* Structure fixed for all versions */
struct rpc_rdma_errvers {
	uint32_t rdma_vers_low;
	uint32_t rdma_vers_high;
};

union rpc_rdma_error switch (rpc_rdma_errcode err) {
	case ERR_VERS:
		rpc_rdma_errvers range;
	case ERR_CHUNK:
		void;
};

/*
 * Procedures (Section 4.2.4)
 */
enum rdma_proc {
	RDMA_MSG = 0,     /* Value fixed for all versions */
	RDMA_NOMSG = 1,   /* Value fixed for all versions */
	RDMA_MSGP = 2,    /* Not to be used */
	RDMA_DONE = 3,    /* Not to be used */
	RDMA_ERROR = 4    /* Value fixed for all versions */
};

/* The position of the proc discriminator field is
 * fixed for all versions */
union rdma_body switch (rdma_proc proc) {
	case RDMA_MSG:
		rpc_rdma_header rdma_msg;
	case RDMA_NOMSG:
		rpc_rdma_header_nomsg rdma_nomsg;
	case RDMA_MSGP:   /* Not to be used */
		rpc_rdma_header_padded rdma_msgp;
	case RDMA_DONE:   /* Not to be used */
		void;
	case RDMA_ERROR:
		rpc_rdma_error rdma_error;
};

/*
 * Fixed header fields (Section 4.2)
 */
struct rdma_msg {
	uint32_t  rdma_xid;      /* Position fixed for all versions */
	uint32_t  rdma_vers;     /* Position fixed for all versions */
	uint32_t  rdma_credit;   /* Position fixed for all versions */
	rdma_body rdma_body;
};
