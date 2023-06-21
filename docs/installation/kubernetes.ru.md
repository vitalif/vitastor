[Документация](../../README-ru.md#документация) → Установка → Kubernetes CSI

-----

[Read in English](kubernetes.en.md)

# Kubernetes CSI

У Vitastor есть CSI-плагин для Kubernetes, поддерживающий RWO, а также блочные RWX, тома.

Для установки возьмите манифесты из директории [csi/deploy/](../../csi/deploy/), поместите
вашу конфигурацию подключения к Vitastor в [csi/deploy/001-csi-config-map.yaml](../../csi/deploy/001-csi-config-map.yaml),
настройте StorageClass в [csi/deploy/009-storage-class.yaml](../../csi/deploy/009-storage-class.yaml)
и примените все `NNN-*.yaml` к вашей инсталляции Kubernetes.

```
for i in ./???-*.yaml; do kubectl apply -f $i; done
```

После этого вы сможете создавать PersistentVolume. Пример смотрите в файле [csi/deploy/example-pvc.yaml](../../csi/deploy/example-pvc.yaml).
