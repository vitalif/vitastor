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

After that you'll be able to create PersistentVolumes. See example in [csi/deploy/example-pvc.yaml](../../csi/deploy/example-pvc.yaml).
