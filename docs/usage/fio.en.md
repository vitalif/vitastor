[Documentation](../../README.md#documentation) → Usage → fio driver

-----

[Читать на русском](fio.ru.md)

# fio driver

[fio](https://fio.readthedocs.io/en/latest/fio_doc.html) (Flexible I/O tester) is the
best utility for benchmarking block devices.

Vitastor has a fio driver which can be installed from the package vitastor-fio.

Use the following command as an example to run tests with fio against a Vitastor cluster:

```
fio -thread -ioengine=libfio_vitastor.so -name=test -bs=4M -direct=1 -iodepth=16 -rw=write -etcd=10.115.0.10:2379/v3 -image=testimg
```

If you don't want to access your image by name, you can specify pool number, inode number and size
(`-pool=1 -inode=1 -size=400G`) instead of the image name (`-image=testimg`).

See exact fio commands to use for benchmarking [here](../performance/understanding.en.md#команды-fio).
