[Documentation](../../README.md#documentation) → Usage → NBD

-----

[Читать на русском](nbd.ru.md)

# NBD

NBD stands for "Network Block Device", but in fact it also functions as "BUSE"
(Block Device in UserSpace). NBD is currently required to mount Vitastor via kernel.
NBD slighly lowers the performance due to additional overhead, but performance still
remains decent (see an example [here](../performance/comparison1.en.md#vitastor-0-4-0-nbd)).

Vitastor Kubernetes CSI driver is based on NBD.

## Map image

To create a local block device for a Vitastor image run:

```
vitastor-nbd map --etcd_address 10.115.0.10:2379/v3 --image testimg
```

It will output a block device name like /dev/nbd0 which you can then use as a normal disk.

You can also use `--pool <POOL> --inode <INODE> --size <SIZE>` instead of `--image <IMAGE>` if you want.

## Unmap image

To unmap the device run:

```
vitastor-nbd unmap /dev/nbd0
```
