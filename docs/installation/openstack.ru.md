[Документация](../../README-ru.md#документация) → Установка → OpenStack

-----

[Read in English](openstack.en.md)

# OpenStack

Чтобы подключить Vitastor к OpenStack:

- Установите пакеты vitastor-client, libvirt и QEMU из DEB или RPM репозитория Vitastor
- Примените патч `patches/nova-21.diff` или `patches/nova-23.diff` к вашей инсталляции Nova.
  nova-21.diff подходит для Nova 21-22, nova-23.diff подходит для Nova 23-24.
- Скопируйте `patches/cinder-vitastor.py` в инсталляцию Cinder как `cinder/volume/drivers/vitastor.py`
- Создайте тип томов в cinder.conf (см. ниже)
- Обязательно заблокируйте доступ от виртуальных машин к сети Vitastor (OSD и etcd), т.к. Vitastor (пока) не поддерживает аутентификацию
- Перезапустите Cinder и Nova

Пример конфигурации Cinder:

```
[DEFAULT]
enabled_backends = lvmdriver-1, vitastor-testcluster
# ...

[vitastor-testcluster]
volume_driver = cinder.volume.drivers.vitastor.VitastorDriver
volume_backend_name = vitastor-testcluster
image_volume_cache_enabled = True
volume_clear = none
vitastor_etcd_address = 192.168.7.2:2379
vitastor_etcd_prefix =
vitastor_config_path = /etc/vitastor/vitastor.conf
vitastor_pool_id = 1
image_upload_use_cinder_backend = True
```

Чтобы помещать в Vitastor Glance-образы, нужно использовать
[образы на основе томов Cinder](https://docs.openstack.org/cinder/pike/admin/blockstorage-volume-backed-image.html),
однако, поддержка этой функции ещё не проверялась.
