// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - common functions

#include <sys/time.h>

#include "str_util.h"
#include "nfs_proxy.h"
#include "nfs_kv.h"

nfsstat3 vitastor_nfs_map_err(int err)
{
    return (err == EINVAL ? NFS3ERR_INVAL
        : (err == ENOENT ? NFS3ERR_NOENT
        : (err == ENOSPC ? NFS3ERR_NOSPC
        : (err == EEXIST ? NFS3ERR_EXIST
        : (err == EIO ? NFS3ERR_IO : (err ? NFS3ERR_IO : NFS3_OK))))));
}

nfstime3 nfstime_from_str(const std::string & s)
{
    nfstime3 t;
    auto p = s.find(".");
    if (p != std::string::npos)
    {
        t.seconds = stoull_full(s.substr(0, p), 10);
        t.nseconds = stoull_full(s.substr(p+1), 10);
        p = s.size()-p-1;
        for (; p < 9; p++)
            t.nseconds *= 10;
        for (; p > 9; p--)
            t.nseconds /= 10;
    }
    else
        t.seconds = stoull_full(s, 10);
    return t;
}

std::string nfstime_to_str(nfstime3 t)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%u.%09u", t.seconds, t.nseconds);
    return buf;
}

int kv_map_type(const std::string & type)
{
    return (type == "" || type == "file" ? NF3REG :
        (type == "dir" ? NF3DIR :
        (type == "blk" ? NF3BLK :
        (type == "chr" ? NF3CHR :
        (type == "link" ? NF3LNK :
        (type == "sock" ? NF3SOCK :
        (type == "fifo" ? NF3FIFO : -1)))))));
}

fattr3 get_kv_attributes(nfs_client_t *self, uint64_t ino, json11::Json attrs)
{
    auto type = kv_map_type(attrs["type"].string_value());
    auto mode = attrs["mode"].uint64_value();
    auto nlink = attrs["nlink"].uint64_value();
    nfstime3 mtime = nfstime_from_str(attrs["mtime"].string_value());
    nfstime3 atime = attrs["atime"].is_null() ? mtime : nfstime_from_str(attrs["atime"].string_value());
    // FIXME In theory we could store the binary structure itself instead of JSON
    return (fattr3){
        .type = (type == 0 ? NF3REG : (ftype3)type),
        .mode = (attrs["mode"].is_null() ? (type == NF3DIR ? 0755 : 0644) : (uint32_t)mode),
        .nlink = (nlink == 0 ? 1 : (uint32_t)nlink),
        .uid = (uint32_t)attrs["uid"].uint64_value(),
        .gid = (uint32_t)attrs["gid"].uint64_value(),
        .size = (type == NF3DIR ? 4096 : attrs["size"].uint64_value()),
        .used = (type == NF3DIR ? 4096 : attrs["alloc"].uint64_value()),
        .rdev = (type == NF3BLK || type == NF3CHR
            ? (specdata3){ (uint32_t)attrs["major"].uint64_value(), (uint32_t)attrs["minor"].uint64_value() }
            : (specdata3){}),
        .fsid = self->parent->fsid,
        .fileid = ino,
        .atime = atime,
        .mtime = mtime,
        .ctime = mtime,
    };
}

std::string kv_direntry_key(uint64_t dir_ino, const std::string & filename)
{
    // encode as: d <length> <hex dir_ino> / <filename>
    char key[24] = { 0 };
    snprintf(key, sizeof(key), "d-%jx/", dir_ino);
    int n = strnlen(key, sizeof(key)-1) - 3;
    if (n < 10)
        key[1] = '0'+n;
    else
        key[1] = 'A'+(n-10);
    return (char*)key + filename;
}

std::string kv_direntry_filename(const std::string & key)
{
    // decode as: d <length> <hex dir_ino> / <filename>
    auto pos = key.find("/");
    if (pos != std::string::npos)
        return key.substr(pos+1);
    return key;
}

std::string kv_inode_key(uint64_t ino)
{
    char key[24] = { 0 };
    snprintf(key, sizeof(key), "i-%jx", ino);
    int n = strnlen(key, sizeof(key)-1) - 2;
    if (n < 10)
        key[1] = '0'+n;
    else
        key[1] = 'A'+(n-10);
    return std::string(key, n+2);
}

std::string kv_fh(uint64_t ino)
{
    return "S"+std::string((char*)&ino, 8);
}

uint64_t kv_fh_inode(const std::string & fh)
{
    if (fh.size() == 1 && fh[0] == 'R')
    {
        return 1;
    }
    else if (fh.size() == 9 && fh[0] == 'S')
    {
        return *(uint64_t*)&fh[1];
    }
    else if (fh.size() > 17 && fh[0] == 'I')
    {
        return *(uint64_t*)&fh[fh.size()-8];
    }
    return 0;
}

bool kv_fh_valid(const std::string & fh)
{
    return fh == NFS_ROOT_HANDLE || fh.size() == 9 && fh[0] == 'S' || fh.size() > 17 && fh[0] == 'I';
}
