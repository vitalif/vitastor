// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy - common FSSTAT, FSINFO, PATHCONF

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

// Get file system statistics
int nfs3_fsstat_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    //FSSTAT3args *args = (FSSTAT3args*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] FSSTAT\n", self->nfs_fd);
    FSSTAT3res *reply = (FSSTAT3res*)rop->reply;
    uint64_t tbytes = 0, fbytes = 0;
    auto pst_it = self->parent->pool_stats.find(self->parent->default_pool_id);
    if (pst_it != self->parent->pool_stats.end())
    {
        auto ttb = pst_it->second["total_raw_tb"].number_value();
        auto ftb = (pst_it->second["total_raw_tb"].number_value() - pst_it->second["used_raw_tb"].number_value());
        tbytes = ttb / pst_it->second["raw_to_usable"].number_value() * ((uint64_t)2<<40);
        fbytes = ftb / pst_it->second["raw_to_usable"].number_value() * ((uint64_t)2<<40);
    }
    *reply = (FSSTAT3res){
        .status = NFS3_OK,
        .resok = (FSSTAT3resok){
            .obj_attributes = {
                .attributes_follow = 0,
                //.attributes = get_root_attributes(self),
            },
            .tbytes = tbytes, // total bytes
            .fbytes = fbytes, // free bytes
            .abytes = fbytes, // available bytes
            .tfiles = (size3)1 << (63-POOL_ID_BITS), // maximum total files
            .ffiles = (size3)1 << (63-POOL_ID_BITS), // free files
            .afiles = (size3)1 << (63-POOL_ID_BITS), // available files
            .invarsec = 0,
        },
    };
    rpc_queue_reply(rop);
    return 0;
}

int nfs3_fsinfo_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    FSINFO3args *args = (FSINFO3args*)rop->request;
    FSINFO3res *reply = (FSINFO3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] FSINFO %s\n", self->nfs_fd, std::string(args->fsroot).c_str());
    if (args->fsroot != NFS_ROOT_HANDLE)
    {
        *reply = (FSINFO3res){ .status = NFS3ERR_INVAL };
    }
    else
    {
        // Fill info
        *reply = (FSINFO3res){
            .status = NFS3_OK,
            .resok = (FSINFO3resok){
                .obj_attributes = {
                    .attributes_follow = 0,
                    //.attributes = get_root_attributes(self),
                },
                .rtmax = 128*1024*1024,
                .rtpref = 128*1024*1024,
                .rtmult = 4096,
                .wtmax = 128*1024*1024,
                .wtpref = 128*1024*1024,
                .wtmult = 4096,
                .dtpref = 128,
                .maxfilesize = 0x7fffffffffffffff,
                .time_delta = {
                    .seconds = 1,
                    .nseconds = 0,
                },
                .properties = FSF3_SYMLINK | FSF3_HOMOGENEOUS,
            },
        };
    }
    rpc_queue_reply(rop);
    return 0;
}

int nfs3_pathconf_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    PATHCONF3args *args = (PATHCONF3args*)rop->request;
    PATHCONF3res *reply = (PATHCONF3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] PATHCONF %s\n", self->nfs_fd, std::string(args->object).c_str());
    if (args->object != NFS_ROOT_HANDLE)
    {
        *reply = (PATHCONF3res){ .status = NFS3ERR_INVAL };
    }
    else
    {
        // Fill info
        *reply = (PATHCONF3res){
            .status = NFS3_OK,
            .resok = (PATHCONF3resok){
                .obj_attributes = {
                    // Without at least one reference to a non-constant value (local variable or something else),
                    // with gcc 8 we get "internal compiler error: side-effects element in no-side-effects CONSTRUCTOR" here
                    // FIXME: get rid of this after raising compiler requirement
                    .attributes_follow = 0,
                    //.attributes = get_root_attributes(self),
                },
                .linkmax = 0,
                .name_max = 255,
                .no_trunc = TRUE,
                .chown_restricted = FALSE,
                .case_insensitive = FALSE,
                .case_preserving = TRUE,
            },
        };
    }
    rpc_queue_reply(rop);
    return 0;
}
