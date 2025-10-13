[Documentation](../../README.md#documentation) → Installation → OpenStack

-----

[Читать на русском](openstack.ru.md)

# OpenStack

To enable Vitastor support in an OpenStack installation:

- Install vitastor-client, patched QEMU and libvirt packages from Vitastor DEB or RPM repository
- Use `patches/nova-21.diff` or `patches/nova-23.diff` to patch your Nova installation.
  Patch 21 fits Nova 21-22, patch 23 fits Nova 23-24.
- Install `patches/cinder-vitastor.py` as `..../cinder/volume/drivers/vitastor.py`
- Define a volume type in cinder.conf (see below)
- Block network access from VMs to Vitastor network (to OSDs and etcd),
  because Vitastor doesn't support authentication
- Restart Cinder and Nova

Cinder volume type configuration example:

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
vitastor_etcd_prefix = /vitastor
vitastor_config_path = /etc/vitastor/vitastor.conf
vitastor_pool_id = 1
image_upload_use_cinder_backend = True
```

To put Glance images in Vitastor, use [volume-backed images](https://docs.openstack.org/cinder/pike/admin/blockstorage-volume-backed-image.html),
although the support has not been verified yet.
