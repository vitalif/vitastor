[Documentation](../../README.md#documentation) → Installation → Kubernetes CSI

-----

[Читать на русском](kubernetes.ru.md)

# Kubernetes CSI

Vitastor has a CSI plugin for Kubernetes which supports block-based and VitastorFS-based volumes.

Block-based volumes may be formatted and mounted with a normal FS (ext4 or xfs). Such volumes
only support RWO (ReadWriteOnce) mode.

Block-based volumes may also be left without FS and attached into the container as a block
device. Such volumes also support RWX (ReadWriteMany) mode.

VitastorFS-based volumes use a clustered file system and support FS-based RWX (ReadWriteMany)
mode. However, such volumes don't support quotas and snapshots.

To deploy the CSI plugin, take manifests from [csi/deploy/](../../csi/deploy/) directory, put your
Vitastor configuration in [001-csi-config-map.yaml](../../csi/deploy/001-csi-config-map.yaml),
configure storage class in [009-storage-class.yaml](../../csi/deploy/009-storage-class.yaml)
and apply all `NNN-*.yaml` manifests to your Kubernetes installation:

```
for i in ./???-*.yaml; do kubectl apply -f $i; done
```

After that you'll be able to create PersistentVolumes.

**Important:** For best experience, use Linux kernel at least 5.15 with [VDUSE](../usage/qemu.en.md#vduse)
kernel modules enabled (vdpa, vduse, virtio-vdpa). If your distribution doesn't
have them pre-built - build them yourself ([instructions](../usage/qemu.en.md#vduse)),
I promise it's worth it :-). When VDUSE is unavailable, CSI driver uses [NBD](../usage/nbd.en.md)
to map Vitastor devices. NBD is slower and, with kernels older than 5.19, unmountable
if the cluster becomes unresponsible.

## Features

Vitastor CSI supports:
- Kubernetes starting with 1.20 (or 1.17 for older vitastor-csi <= 1.1.0)
- Block-based FS-formatted RWO (ReadWriteOnce) volumes. Example: [PVC](../../csi/deploy/example-pvc.yaml), [pod](../../csi/deploy/example-test-pod.yaml)
- Raw block RWX (ReadWriteMany) volumes. Example: [PVC](../../csi/deploy/example-pvc-block.yaml), [pod](../../csi/deploy/example-test-pod-block.yaml)
- VitastorFS-based volumes RWX (ReadWriteMany) volumes. Example: [storage class](../../csi/deploy/example-storage-class-fs.yaml)
- Volume expansion
- Volume snapshots. Example: [snapshot class](../../csi/deploy/example-snapshot-class.yaml), [snapshot](../../csi/deploy/example-snapshot.yaml), [clone](../../csi/deploy/example-snapshot-clone.yaml)
- [VDUSE](../usage/qemu.en.md#vduse) (preferred) and [NBD](../usage/nbd.en.md) device mapping methods
- Upgrades with VDUSE - new handler processes are restarted when CSI pods are restarted themselves
- VDUSE daemon auto-restart - handler processes are automatically restarted if they crash due to a bug in Vitastor client code
- Multiple clusters by using multiple configuration files in ConfigMap.

Remember that to use snapshots with CSI you also have to install [Snapshot Controller and CRDs](https://kubernetes-csi.github.io/docs/snapshot-controller.html#deployment).
