[Documentation](../../README.md#documentation) → Usage → VitastorFS and pseudo-FS

-----

[Читать на русском](nfs.ru.md)

# VitastorFS and pseudo-FS

Vitastor has two file system implementations. Both can be used via `vitastor-nfs`.

Commands:
- [mount](#mount)
- [start](#start)
- [upgrade](#upgrade)
- [defrag](#defrag)

## Pseudo-FS

Simplified pseudo-FS proxy is used for file-based image access emulation. It's not
suitable as a full-featured file system: it lacks a lot of FS features, it stores
all file/image metadata in memory and in etcd. So it's fine for hundreds or thousands
of large files/images, but not for millions.

Pseudo-FS proxy is intended for environments where other block volume access methods
can't be used or impose additional restrictions - for example, VMWare. NFS is better
for VMWare than, for example, iSCSI, because with iSCSI, VMWare puts all VM images
into one large shared block image in its own VMFS file system, and with NFS, VMWare
doesn't use VMFS and puts each VM disk in a regular file which is equal to one
Vitastor block image, just as originally intended.

To use Vitastor pseudo-FS locally, run `vitastor-nfs mount --block /mnt/vita`.

Also you can start the network server:

```
vitastor-nfs start --block --etcd_address 192.168.5.10:2379 --portmap 0 --port 2050 --pool testpool
```

To mount the FS exported by this server, run:

```
mount server:/ /mnt/ -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
```

## VitastorFS

VitastorFS is a full-featured clustered (Read-Write-Many) file system. It supports most POSIX
features like hierarchical organization, symbolic links, hard links, quick renames and so on.

VitastorFS metadata is stored in a Parallel Optimistic B-Tree key-value database,
implemented over a regular Vitastor block volume. Directory entries and inodes
are stored in a simple human-readable JSON format in the B-Tree. `vitastor-kv` tool
can be used to inspect the database.

To use VitastorFS:

1. Create a pool or choose an existing empty pool for FS data
2. Create an image for FS metadata, preferably in a faster (SSD or replica-HDD) pool,
   but you can create it in the data pool too if you want (image size doesn't matter):
   `vitastor-cli create -s 10G -p fastpool testfs`
3. Mark data pool as an FS pool: `vitastor-cli modify-pool --used-for-fs testfs data-pool`
4. Either mount the FS: `vitastor-nfs mount --fs testfs --pool data-pool /mnt/vita`
5. Or start the NFS server: `vitastor-nfs start --fs testfs --pool data-pool`

### Supported POSIX features

- Read-after-write semantics (read returns new data immediately after write)
- Linear and random read and write
- Writing outside current file size
- Hierarchical structure, immediate rename of files and directories
- File size change support (truncate)
- Permissions (chmod/chown)
- Flushing data to stable storage (if required) (fsync)
- Symbolic links
- Hard links
- Special files (devices, sockets, named pipes)
- File modification and attribute change time tracking (mtime and ctime)
- Modification time (mtime) and last access time (atime) change support (utimes)
- Correct handling of directory listing during file creation/deletion

### Limitations

POSIX features currently not implemented in VitastorFS:
- File locking is not supported
- Actually used space is not counted, so `du` always reports apparent file sizes
  instead of actually allocated space
- Access times (`atime`) are not tracked (like `-o noatime`)
- Modification time (`mtime`) is updated lazily every second (like `-o lazytime`)

Other notable missing features which should be addressed in the future:
- Inode ID reuse. Currently inode IDs always grow, the limit is 2^48 inodes, so
  in theory you may hit it if you create and delete a very large number of files
- Compaction of the key-value B-Tree. Current implementation never merges or deletes
  B-Tree blocks, so B-Tree may become bloated over time. Currently you can
  use `vitastor-kv dumpjson` & `loadjson` commands to recreate the index in such
  situations.
- Filesystem check tool. VitastorFS doesn't have journal because it would impose a
  severe performance hit, optimistic CAS-based transactions are used instead of it.
  So, again, in theory an abnormal shutdown of the FS server may leave some garbage
  in the DB. The FS is implemented is such way that this garbage doesn't affect its
  function, but having a tool to clean it up still seems a right thing to do.

## Horizontal scaling

Linux NFS 3.0 client doesn't support built-in scaling or failover, i.e. you can't
specify multiple server addresses when mounting the FS.

However, you can use any regular TCP load balancing over multiple NFS servers.
It's absolutely safe with `immediate_commit=all` and `client_enable_writeback=false`
settings, because Vitastor NFS proxy doesn't keep uncommitted data in memory
with these settings. But it may even work without `immediate_commit=all` because
the Linux NFS client repeats all uncommitted writes if it loses the connection.

## RDMA

vitastor-nfs supports NFS over RDMA, which, in theory, should also allow to use
VitastorFS from GPUDirect.

You can test NFS-RDMA even if you don't have an RDMA NIC using SoftROCE:

1. First, add SoftROCE device on both servers: `rdma link add rxe0 type rxe netdev eth0`.
   Here, `rdma` utility is a part the iproute2 package, and `eth0` should be replaced with
   the name of your Ethernet NIC.

2. Start vitastor-nfs with RDMA: `vitastor-nfs start (--fs <NAME> | --block) --pool <POOL> --port 20049 --nfs_rdma 20049 --portmap 0`

3. Mount the FS: `mount 192.168.0.10:/mnt/test/ /mnt/vita/ -o port=20049,mountport=20049,nfsvers=3,soft,nolock,rdma`

## Commands

### mount

`vitastor-nfs (--fs <NAME> | --block) [-o <OPT>] mount <MOUNTPOINT>`

Start local filesystem server and mount file system to <MOUNTPOINT>.

Use regular `umount <MOUNTPOINT>` to unmount the FS.

The server will be automatically stopped when the FS is unmounted.

- `-o|--options <OPT>` - Pass additional NFS mount options (ex.: -o async).

### start

`vitastor-nfs (--fs <NAME> | --block) start`

Start network NFS server. Options:

| <!-- -->               | <!-- -->                                                                                                                    |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `--bind <IP>`          | bind service to \<IP> address (default 0.0.0.0)                                                                             |
| `--port <PORT>`        | use port \<PORT> for NFS services (default is 2049). Specify "auto" to auto-select and print port                           |
| `--portmap 0`          | do not listen on port 111 (portmap/rpcbind, requires root)                                                                  |
| `--nfs_rdma <PORT>`    | enable NFS-RDMA at RDMA-CM port \<PORT> (you can try 20049). If RDMA is enabled and --port is set to 0, TCP will be disabled |
| `--nfs_rdma_credit 16` | maximum operation credit for RDMA clients (max iodepth)                                                                     |
| `--nfs_rdma_send 1024` | maximum RDMA send operation count (should be larger than iodepth)                                                           |
| `--nfs_rdma_alloc 1M`  | RDMA memory allocation rounding                                                                                             |
| `--nfs_rdma_gc 64M`    | maximum unused RDMA buffers                                                                                                 |

### upgrade

`vitastor-nfs --fs <NAME> upgrade`

Upgrade FS metadata. Can be run online, but server(s) should be restarted after upgrade.

### defrag

`vitastor-nfs --fs <NAME> defrag [OPTIONS] [--dry-run]`

Defragment volumes used for small file storage having more than \<defrag_percent> %
of data removed. Can be run online.

In VitastorFS, small files are stored in large "volumes" / "shared inodes" one
after another. When you delete or extend such files, they are moved and garbage is left
behind. Defragmentation removes garbage and moves data still in use to new volumes.

Options:

| <!-- -->                   | <!-- -->                                                                |
|----------------------------|------------------------------------------------------------------------ |
| `--volume_untouched 86400` | Defragment volumes last appended to at least this number of seconds ago |
| `--defrag_percent 50`      | Defragment volumes with at least this % of removed data                 |
| `--defrag_block_count 16`  | Read this number of pool blocks at once during defrag                   |
| `--defrag_iodepth 16`      | Move up to this number of files in parallel during defrag               |
| `--trace`                  | Print verbose defragmentation status                                    |
| `--dry-run`                | Skip modifications, only print status                                   |
| `--recalc-stats`           | Recalculate all volume statistics                                       |
| `--include-empty`          | Include old and empty volumes; make sure to restart NFS servers before using it |
| `--no-rm`                  | Move, but do not delete data                                            |

## Common options

| <!-- -->           | <!-- -->                                                 |
|--------------------|----------------------------------------------------------|
| `--fs <NAME>`      | use VitastorFS with metadata in image \<NAME>            |
| `--block`          | use pseudo-FS presenting images as files                 |
| `--pool <POOL>`    | use \<POOL> as default pool for new files                |
| `--subdir <DIR>`   | export \<DIR> instead of root directory (pseudo-FS only) |
| `--nfspath <PATH>` | set NFS export path to \<PATH> (default is /)            |
| `--pidfile <FILE>` | write process ID to the specified file                   |
| `--logfile <FILE>` | log to the specified file                                |
| `--foreground 1`   | stay in foreground, do not daemonize                     |
