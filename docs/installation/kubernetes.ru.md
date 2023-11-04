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

После этого вы сможете создавать PersistentVolume.

## Возможности

CSI-плагин Vitastor поддерживает:
- Версии Kubernetes, начиная с 1.20 (или с 1.17 для более старых vitastor-csi <= 1.1.0)
- Файловые RWO (ReadWriteOnce) тома. Пример: [PVC](../../csi/deploy/example-pvc.yaml), [под](../../csi/deploy/example-test-pod.yaml)
- Сырые блочные RWX (ReadWriteMany) тома. Пример: [PVC](../../csi/deploy/example-pvc-block.yaml), [под](../../csi/deploy/example-test-pod-block.yaml)
- Расширение размера томов
- Снимки томов. Пример: [класс снимков](../../csi/deploy/example-snapshot-class.yaml), [снимок](../../csi/deploy/example-snapshot.yaml), [клон снимка](../../csi/deploy/example-snapshot-clone.yaml)

Не забывайте, что для использования снимков нужно сначала установить [контроллер снимков и CRD](https://kubernetes-csi.github.io/docs/snapshot-controller.html#deployment).
