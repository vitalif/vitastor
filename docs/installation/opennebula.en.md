[Documentation](../../README.md#documentation) → Installation → OpenNebula

-----

[Читать на русском](opennebula.ru.md)

## Automatic Installation

OpenNebula plugin is packaged as `opennebula-vitastor` Debian and RPM package since Vitastor 1.9.0. So:

- Run `apt-get install opennebula-vitastor` or `yum install vitastor` after installing OpenNebula on all nodes
- Check that it prints "OK, Vitastor OpenNebula patches successfully applied" or "OK, Vitastor OpenNebula patches are already applied"
- If it does not, refer to [Manual Installation](#manual-installation) and apply configuration file changes manually
- [Block VM access to Vitastor cluster](#block-vm-access-to-vitastor-cluster)

## Manual Installation

Install OpenNebula. Then, on each node:

- Copy [opennebula/remotes](../../opennebula/remotes) into `/var/lib/one` recursively: `cp -r opennebula/remotes /var/lib/one/`
- Copy [opennebula/sudoers.d](../../opennebula/sudoers.d) to `/etc`: `cp -r opennebula/sudoers.d /etc/`
- Apply [downloader-vitastor.sh.diff](../../opennebula/remotes/datastore/vitastor/downloader-vitastor.sh.diff) to `/var/lib/one/remotes/datastore/downloader.sh`:
  `patch /var/lib/one/remotes/datastore/downloader.sh < opennebula/remotes/datastore/vitastor/downloader-vitastor.sh.diff` - or read the patch and apply the same change manually
- Add `kvm-vitastor` to `LIVE_DISK_SNAPSHOTS` in `/etc/one/vmm_exec/vmm_execrc`
- If on Debian or Ubuntu (and AppArmor is used), add Vitastor config file path(s) to `/etc/apparmor.d/local/abstractions/libvirt-qemu`: for example,
  `echo '  "/etc/vitastor/vitastor.conf" r,' >> /etc/apparmor.d/local/abstractions/libvirt-qemu`
- Apply changes to `/etc/one/oned.conf`

### oned.conf changes

1. Add deploy script override in kvm VM_MAD: add `-l deploy.vitastor` to ARGUMENTS:

```diff
 VM_MAD = [
     NAME           = "kvm",
     SUNSTONE_NAME  = "KVM",
     EXECUTABLE     = "one_vmm_exec",
-    ARGUMENTS      = "-t 15 -r 0 kvm -p",
+    ARGUMENTS      = "-t 15 -r 0 kvm -p -l deploy=deploy.vitastor",
     DEFAULT        = "vmm_exec/vmm_exec_kvm.conf",
     TYPE           = "kvm",
     KEEP_SNAPSHOTS = "yes",
     LIVE_RESIZE    = "yes",
     SUPPORT_SHAREABLE    = "yes",
     IMPORTED_VMS_ACTIONS = "terminate, terminate-hard, hold, release, suspend,
         resume, delete, reboot, reboot-hard, resched, unresched, disk-attach,
         disk-detach, nic-attach, nic-detach, snapshot-create, snapshot-delete,
         resize, updateconf, update"
 ]
```

2. Add `vitastor` to TM_MAD.ARGUMENTS and DATASTORE_MAD.ARGUMENTS:

```diff
 TM_MAD = [
     EXECUTABLE = "one_tm",
-    ARGUMENTS = "-t 15 -d dummy,lvm,shared,fs_lvm,fs_lvm_ssh,qcow2,ssh,ceph,dev,vcenter,iscsi_libvirt"
+    ARGUMENTS = "-t 15 -d dummy,lvm,shared,fs_lvm,fs_lvm_ssh,qcow2,ssh,ceph,vitastor,dev,vcenter,iscsi_libvirt"
 ]

 DATASTORE_MAD = [
     EXECUTABLE = "one_datastore",
-    ARGUMENTS  = "-t 15 -d dummy,fs,lvm,ceph,dev,iscsi_libvirt,vcenter,restic,rsync -s shared,ssh,ceph,fs_lvm,fs_lvm_ssh,qcow2,vcenter"
+    ARGUMENTS  = "-t 15 -d dummy,fs,lvm,ceph,vitastor,dev,iscsi_libvirt,vcenter,restic,rsync -s shared,ssh,ceph,vitastor,fs_lvm,fs_lvm_ssh,qcow2,vcenter"
 ]
```

3. Add INHERIT_DATASTORE_ATTR for two Vitastor attributes:

```
INHERIT_DATASTORE_ATTR = "VITASTOR_CONF"
INHERIT_DATASTORE_ATTR = "IMAGE_PREFIX"
```

4. Add TM_MAD_CONF and DS_MAD_CONF for Vitastor:

```
TM_MAD_CONF = [
    NAME = "vitastor", LN_TARGET = "NONE", CLONE_TARGET = "SELF", SHARED = "YES",
    DS_MIGRATE = "NO", DRIVER = "raw", ALLOW_ORPHANS="format",
    TM_MAD_SYSTEM = "ssh,shared", LN_TARGET_SSH = "SYSTEM", CLONE_TARGET_SSH = "SYSTEM",
    DISK_TYPE_SSH = "FILE", LN_TARGET_SHARED = "NONE",
    CLONE_TARGET_SHARED = "SELF", DISK_TYPE_SHARED = "FILE"
]

DS_MAD_CONF = [
    NAME = "vitastor",
    REQUIRED_ATTRS = "DISK_TYPE,BRIDGE_LIST",
    PERSISTENT_ONLY = "NO",
    MARKETPLACE_ACTIONS = "export"
]
```

## Create Datastores

Example Image and System Datastore definitions:
[opennebula/vitastor-imageds.conf](../../opennebula/vitastor-imageds.conf) and
[opennebula/vitastor-systemds.conf](../../opennebula/vitastor-systemds.conf).

Change parameters to your will:

- POOL_NAME is Vitastor pool name to store images.
- IMAGE_PREFIX is a string prepended to all Vitastor image names.
- BRIDGE_LIST is a list of hosts with access to Vitastor cluster, mostly used for image (not system) datastore operations.
- VITASTOR_CONF is the path to cluster configuration. Note that it should be also added to `/etc/apparmor.d/local/abstractions/libvirt-qemu` if you use AppArmor.
- STAGING_DIR is a temporary directory used when importing external images. Should have free space sufficient for downloading external images.

Then create datastores using `onedatastore create vitastor-imageds.conf` and `onedatastore create vitastor-systemds.conf` (or use UI).

## Block VM access to Vitastor cluster

Vitastor doesn't support any authentication yet, so you MUST block VM guest access to the Vitastor cluster at the network level.

If you use VLAN networking for VMs - make sure you use different VLANs for VMs and hypervisor/storage network and
block access between them using your firewall/switch configuration.

If you use something more stupid like bridged networking, you probably have to use manual firewall/iptables setup
to only allow access to Vitastor from hypervisor IPs.

Also you need to switch network to "Bridged & Security Groups" and enable IP spoofing filters in OpenNebula.
Problem is that OpenNebula's IP spoofing filter doesn't affect local interfaces of the hypervisor i.e. when
it's enabled a VM can't talk to other VMs or to the outer world using a spoofed IP, but it CAN talk to the
hypervisor if it takes an IP from its subnet. To fix that you also need some more iptables.

So the complete "stupid" bridged network filter setup could look like the following
(here `10.0.3.0/24` is the VM subnet and `10.0.2.0/24` is the hypervisor subnet):

```
# Allow incoming traffic from physical device
iptables -A INPUT -m physdev --physdev-in eth0 -j ACCEPT
# Do not allow incoming traffic from VMs, but not from VM subnet
iptables -A INPUT ! -s 10.0.3.0/24 -i onebr0 -j DROP
# Drop traffic from VMs to hypervisor/storage subnet
iptables -I FORWARD 1 -s 10.0.3.0/24 -d 10.0.2.0/24 -j DROP
```

## Testing

The OpenNebula plugin includes quite a bit of bash scripts, so here's their description to get an idea about what they actually do.

| Script                  | Action                                    | How to Test                                                                          |
| ----------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------ |
| vmm/kvm/deploy.vitastor | Start a VM                                | Create and start a VM with Vitastor disk(s): persistent / non-persistent / volatile. |
| datastore/clone         | Copy an image as persistent               | Create a VM template and instantiate it as persistent.                               |
| datastore/cp            | Import an external image                  | Import a VM template with images from Marketplace.                                   |
| datastore/export        | Export an image as URL                    | Probably: export a VM template with images to Marketplace.                           |
| datastore/mkfs          | Create an image with FS                   | Storage → Images → Create → Type: Datablock, Location: Empty disk image, Filesystem: Not empty. |
| datastore/monitor       | Monitor used space in image datastore     | Check reported used/free space in image datastore list.                              |
| datastore/rm            | Remove a persistent image                 | Storage → Images → Select an image → Delete.                                         |
| datastore/snap_delete   | Delete a snapshot of a persistent image   | Storage → Images → Select an image → Select a snapshot → Delete; <br> To create an image with snapshot: attach a persistent image to a VM; create a snapshot; detach the image. |
| datastore/snap_flatten  | Revert an image to snapshot and delete other snapshots | Storage → Images → Select an image → Select a snapshot → Flatten.       |
| datastore/snap_revert   | Revert an image to snapshot               | Storage → Images → Select an image → Select a snapshot → Revert.                     |
| datastore/stat          | Get virtual size of an image in MB        | No idea. Seems to be unused both in Vitastor and Ceph datastores.                    |
| tm/clone                | Clone a non-persistent image to a VM disk | Attach a non-persistent image to a VM.                                               |
| tm/context              | Generate a contextualisation VM disk      | Create a VM with enabled contextualisation (default). Common host FS-based version is used in Vitastor and Ceph datastores. |
| tm/cpds                 | Copy a VM disk / its snapshot to an image | Select a VM → Select a disk → Optionally select a snapshot → Save as.                |
| tm/delete               | Delete a cloned or volatile VM disk       | Detach a volatile disk or a non-persistent image from a VM.                          |
| tm/failmigrate          | Handle live migration failure             | No action. Script is empty in Vitastor and Ceph. In other datastores, should roll back actions done by tm/premigrate. |
| tm/ln                   | Attach a persistent image to a VM         | No action. Script is empty in Vitastor and Ceph.                                     |
| tm/mkimage              | Create a volatile disk, maybe with FS     | Attach a volatile disk to a VM, with or without file system.                         |
| tm/mkswap               | Create a volatile swap disk               | Attach a volatile disk to a VM, formatted as swap.                                   |
| tm/monitor              | Monitor used space in system datastore    | Check reported used/free space in system datastore list.                             |
| tm/mv                   | Move a migrated VM disk between hosts     | Migrate a VM between hosts. In Vitastor and Ceph datastores, doesn't do any storage action. |
| tm/mvds                 | Detach a persistent image from a VM       | No action. The opposite of tm/ln. Script is empty in Vitastor and Ceph. In other datastores, script may copy the image from VM host back to the datastore. |
| tm/postbackup           | Executed after backup                     | Seems that the script just removes temporary files after backup. Perform a VM backup and check that temporary files are cleaned up. |
| tm/postbackup_live      | Executed after backup of a running VM     | Same as tm/postbackup, but for a running VM.                                         |
| tm/postmigrate          | Executed after VM live migration          | No action. Only executed for system datastore, so the script tries to call other TMs for other disks. Except that, the script does nothing in Vitastor and Ceph datastores. |
| tm/prebackup            | Actual backup script: backup VM disks     | Set up "rsync" backup datastore → Backup a VM to it.                                 |
| tm/prebackup_live       | Backup VM disks of a running VM           | Same as tm/prebackup, but also does fsfreeze/thaw. So perform a live backup, restore it and check that disks are consistent. |
| tm/premigrate           | Executed before live migration            | No action. Only executed for system datastore, so the script tries to call other TMs for other disks. Except that, the script does nothing in Vitastor and Ceph datastores. |
| tm/resize               | Resize a VM disk                          | Select a VM → Select a non-persistent disk → Resize.                                 |
| tm/restore              | Restore VM disks from backup              | Set up "rsync" backup datastore → Backup a VM to it → Restore it back.               |
| tm/snap_create          | Create a VM disk snapshot                 | Select a VM → Select a disk → Create snapshot.                                       |
| tm/snap_create_live     | Create a VM disk snapshot for a live VM   | Select a running VM → Select a disk → Create snapshot.                               |
| tm/snap_delete          | Delete a VM disk snapshot                 | Select a VM → Select a disk → Select a snapshot → Delete.                            |
| tm/snap_revert          | Revert a VM disk to a snapshot            | Select a VM → Select a disk → Select a snapshot → Revert.                            |
