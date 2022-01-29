[Документация](../../README-ru.md#документация) → Использование → Драйвер fio

-----

[Read in English](fio.en.md)

# Драйвер fio

[fio](https://fio.readthedocs.io/en/latest/fio_doc.html) (Flexible I/O tester) - лучшая
актуальная утилита для тестирования производительности блочных устройств.

В Vitastor есть драйвер для fio, устанавливаемый из пакета vitastor-fio.

Используйте следующую команду как пример для запуска тестов кластера Vitastor через fio:

```
fio -thread -ioengine=libfio_vitastor.so -name=test -bs=4M -direct=1 -iodepth=16 -rw=write -etcd=10.115.0.10:2379/v3 -image=testimg
```

Вместо обращения к образу по имени (`-image=testimg`) можно указать номер пула, номер инода и размер:
`-pool=1 -inode=1 -size=400G`.

Конкретные команды fio для тестирования производительности можно посмотреть [здесь](../performance/understanding.ru.md#команды-fio).
