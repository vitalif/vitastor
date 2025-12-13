[Documentation](../../README.md#documentation) → Installation → Proxmox VE

-----

[Читать на русском](proxmox.ru.md)

# Proxmox VE

To enable Vitastor support in Proxmox Virtual Environment (6.4-9.x are supported):

- Add the corresponding Vitastor Debian repository into sources.list on Proxmox hosts:
  trixie for 9.0+, bookworm for 8.1+, pve8.0 for 8.0, bullseye for 7.4, pve7.3 for 7.3, pve7.2 for 7.2, pve7.1 for 7.1, buster for 6.4
- Install vitastor-client, pve-qemu-kvm, pve-storage-vitastor (* or see note) packages from Vitastor repository
- Define storage in `/etc/pve/storage.cfg` (see below)
- Block network access from VMs to Vitastor network (to OSDs and etcd),
  because Vitastor doesn't support authentication
- Restart pvedaemon: `systemctl restart pvedaemon`

`/etc/pve/storage.cfg` example (the only required option is vitastor_pool, all others
are listed below with their default values; `vitastor_ssd` is Proxmox storage pool id):

```
vitastor: vitastor_ssd
    # pool to put new images into
    vitastor_pool testpool
    # path to the configuration file
    vitastor_config_path /etc/vitastor/vitastor.conf
    # etcd address(es), OPTIONAL, required only if missing in the configuration file
    vitastor_etcd_address 192.168.7.2:2379/v3
    # prefix for keys in etcd
    vitastor_etcd_prefix /vitastor
    # prefix for images
    vitastor_prefix pve/
    # use NBD mounter (only required for containers)
    vitastor_nbd 0
```

\* Note: you can also manually copy [patches/VitastorPlugin.pm](../../patches/VitastorPlugin.pm) to Proxmox hosts
as `/usr/share/perl5/PVE/Storage/Custom/VitastorPlugin.pm` instead of installing pve-storage-vitastor.
