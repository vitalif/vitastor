[Documentation](../../README.md#documentation) → Installation → Kubernetes CSI

-----

[Читать на русском](kubernetes.ru.md)

# Kubernetes CSI

Vitastor has a CSI plugin for Kubernetes which supports RWO (and block RWX) volumes.

To deploy it, take manifests from [csi/deploy/](../../csi/deploy/) directory, put your
Vitastor configuration in [001-csi-config-map.yaml](../../csi/deploy/001-csi-config-map.yaml),
configure storage class in [009-storage-class.yaml](../../csi/deploy/009-storage-class.yaml)
and apply all `NNN-*.yaml` manifests to your Kubernetes installation:

```
for i in ./???-*.yaml; do kubectl apply -f $i; done
```

After that you'll be able to create PersistentVolumes.

## Features

Vitastor CSI supports:
- Kubernetes starting with 1.20 (or 1.17 for older vitastor-csi <= 1.1.0)
- Filesystem RWO (ReadWriteOnce) volumes. Example: [PVC](../../csi/deploy/example-pvc.yaml), [pod](../../csi/deploy/example-test-pod.yaml)
- Raw block RWX (ReadWriteMany) volumes. Example: [PVC](../../csi/deploy/example-pvc-block.yaml), [pod](../../csi/deploy/example-test-pod-block.yaml)
- Volume expansion
- Volume snapshots. Example: [snapshot class](../../csi/deploy/example-snapshot-class.yaml), [snapshot](../../csi/deploy/example-snapshot.yaml), [clone](../../csi/deploy/example-snapshot-clone.yaml)

Remember that to use snapshots with CSI you also have to install [Snapshot Controller and CRDs](https://kubernetes-csi.github.io/docs/snapshot-controller.html#deployment).
