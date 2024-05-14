// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy - common functions

#pragma once

#include "proto/nfs.h"

void nfs_block_procs(nfs_client_t *self);
void nfs_kv_procs(nfs_client_t *self);
int nfs3_fsstat_proc(void *opaque, rpc_op_t *rop);
int nfs3_fsinfo_proc(void *opaque, rpc_op_t *rop);
int nfs3_pathconf_proc(void *opaque, rpc_op_t *rop);
int nfs3_access_proc(void *opaque, rpc_op_t *rop);
int nfs3_null_proc(void *opaque, rpc_op_t *rop);
int nfs3_commit_proc(void *opaque, rpc_op_t *rop);
int mount3_mnt_proc(void *opaque, rpc_op_t *rop);
int mount3_dump_proc(void *opaque, rpc_op_t *rop);
int mount3_umnt_proc(void *opaque, rpc_op_t *rop);
int mount3_umntall_proc(void *opaque, rpc_op_t *rop);
int mount3_export_proc(void *opaque, rpc_op_t *rop);
