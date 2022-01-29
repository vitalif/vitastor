[Documentation](../../README.md#documentation) → Usage → NFS

-----

[Читать на русском](nfs.ru.md)

# NFS

Vitastor has a simplified NFS 3.0 proxy for file-based image access emulation. It's not
suitable as a full-featured file system, at least because all file/image metadata is stored
in etcd and kept in memory all the time - thus you can't put a lot of files in it.

However, NFS proxy is totally fine as a method to provide VM image access and allows to
plug Vitastor into, for example, VMWare. It's important to note that for VMWare it's a much
better access method than iSCSI, because with iSCSI we'd have to put all VM images into one
Vitastor image exported as a LUN to VMWare and formatted with VMFS. VMWare doesn't use VMFS
over NFS.

NFS proxy is stateless if you use immediate_commit=all mode (for SSD with capacitors or
HDDs with disabled cache), so you can run multiple NFS proxies and use a network load
balancer or any failover method you want to in that case.

vitastor-nfs usage:

```
vitastor-nfs [--etcd_address ADDR] [OTHER OPTIONS]

--subdir <DIR>    export images prefixed <DIR>/ (default empty - export all images)
--portmap 0       do not listen on port 111 (portmap/rpcbind, requires root)
--bind <IP>       bind service to <IP> address (default 0.0.0.0)
--nfspath <PATH>  set NFS export path to <PATH> (default is /)
--port <PORT>     use port <PORT> for NFS services (default is 2049)
--pool <POOL>     use <POOL> as default pool for new files (images)
--foreground 1    stay in foreground, do not daemonize
```

Example start and mount commands:

```
vitastor-nfs --etcd_address 192.168.5.10:2379 --portmap 0 --port 2050 --pool testpool
```

```
mount localhost:/ /mnt/ -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
```
