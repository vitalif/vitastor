/*
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#include "rpc_rdma.h"
#include "xdr_impl_inline.h"

bool_t
xdr_xdr_rdma_segment (XDR *xdrs, xdr_rdma_segment *objp)
{
	
	 if (!xdr_uint32_t (xdrs, &objp->handle))
		 return FALSE;
	 if (!xdr_uint32_t (xdrs, &objp->length))
		 return FALSE;
	 if (!xdr_uint64_t (xdrs, &objp->offset))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_xdr_read_chunk (XDR *xdrs, xdr_read_chunk *objp)
{
	
	 if (!xdr_uint32_t (xdrs, &objp->position))
		 return FALSE;
	 if (!xdr_xdr_rdma_segment (xdrs, &objp->target))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_xdr_read_list (XDR *xdrs, xdr_read_list *objp)
{
	
	 if (!xdr_xdr_read_chunk (xdrs, &objp->entry))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->next, sizeof (xdr_read_list), (xdrproc_t) xdr_xdr_read_list))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_xdr_write_chunk (XDR *xdrs, xdr_write_chunk *objp)
{
	
	 if (!xdr_array (xdrs, (char **)&objp->target.target_val, (u_int *) &objp->target.target_len, ~0,
		sizeof (xdr_rdma_segment), (xdrproc_t) xdr_xdr_rdma_segment))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_xdr_write_list (XDR *xdrs, xdr_write_list *objp)
{
	
	 if (!xdr_xdr_write_chunk (xdrs, &objp->entry))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->next, sizeof (xdr_write_list), (xdrproc_t) xdr_xdr_write_list))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_header (XDR *xdrs, rpc_rdma_header *objp)
{
	
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reads, sizeof (xdr_read_list), (xdrproc_t) xdr_xdr_read_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_writes, sizeof (xdr_write_list), (xdrproc_t) xdr_xdr_write_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reply, sizeof (xdr_write_chunk), (xdrproc_t) xdr_xdr_write_chunk))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_header_nomsg (XDR *xdrs, rpc_rdma_header_nomsg *objp)
{
	
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reads, sizeof (xdr_read_list), (xdrproc_t) xdr_xdr_read_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_writes, sizeof (xdr_write_list), (xdrproc_t) xdr_xdr_write_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reply, sizeof (xdr_write_chunk), (xdrproc_t) xdr_xdr_write_chunk))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_header_padded (XDR *xdrs, rpc_rdma_header_padded *objp)
{
	
	 if (!xdr_uint32_t (xdrs, &objp->rdma_align))
		 return FALSE;
	 if (!xdr_uint32_t (xdrs, &objp->rdma_thresh))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reads, sizeof (xdr_read_list), (xdrproc_t) xdr_xdr_read_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_writes, sizeof (xdr_write_list), (xdrproc_t) xdr_xdr_write_list))
		 return FALSE;
	 if (!xdr_pointer (xdrs, (char **)&objp->rdma_reply, sizeof (xdr_write_chunk), (xdrproc_t) xdr_xdr_write_chunk))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_errcode (XDR *xdrs, rpc_rdma_errcode *objp)
{
	
	 if (!xdr_enum (xdrs, (enum_t *) objp))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_errvers (XDR *xdrs, rpc_rdma_errvers *objp)
{
	
	 if (!xdr_uint32_t (xdrs, &objp->rdma_vers_low))
		 return FALSE;
	 if (!xdr_uint32_t (xdrs, &objp->rdma_vers_high))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rpc_rdma_error (XDR *xdrs, rpc_rdma_error *objp)
{
	
	 if (!xdr_rpc_rdma_errcode (xdrs, &objp->err))
		 return FALSE;
	switch (objp->err) {
	case ERR_VERS:
		 if (!xdr_rpc_rdma_errvers (xdrs, &objp->range))
			 return FALSE;
		break;
	case ERR_CHUNK:
		break;
	default:
		return FALSE;
	}
	return TRUE;
}

bool_t
xdr_rdma_proc (XDR *xdrs, rdma_proc *objp)
{
	
	 if (!xdr_enum (xdrs, (enum_t *) objp))
		 return FALSE;
	return TRUE;
}

bool_t
xdr_rdma_body (XDR *xdrs, rdma_body *objp)
{
	
	 if (!xdr_rdma_proc (xdrs, &objp->proc))
		 return FALSE;
	switch (objp->proc) {
	case RDMA_MSG:
		 if (!xdr_rpc_rdma_header (xdrs, &objp->rdma_msg))
			 return FALSE;
		break;
	case RDMA_NOMSG:
		 if (!xdr_rpc_rdma_header_nomsg (xdrs, &objp->rdma_nomsg))
			 return FALSE;
		break;
	case RDMA_MSGP:
		 if (!xdr_rpc_rdma_header_padded (xdrs, &objp->rdma_msgp))
			 return FALSE;
		break;
	case RDMA_DONE:
		break;
	case RDMA_ERROR:
		 if (!xdr_rpc_rdma_error (xdrs, &objp->rdma_error))
			 return FALSE;
		break;
	default:
		return FALSE;
	}
	return TRUE;
}

bool_t
xdr_rdma_msg (XDR *xdrs, rdma_msg *objp)
{
	
	 if (!xdr_uint32_t (xdrs, &objp->rdma_xid))
		 return FALSE;
	 if (!xdr_uint32_t (xdrs, &objp->rdma_vers))
		 return FALSE;
	 if (!xdr_uint32_t (xdrs, &objp->rdma_credit))
		 return FALSE;
	 if (!xdr_rdma_body (xdrs, &objp->rdma_body))
		 return FALSE;
	return TRUE;
}
