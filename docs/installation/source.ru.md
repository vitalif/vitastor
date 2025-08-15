[Документация](../../README-ru.md#документация) → Установка → Сборка из исходных кодов

-----

[Read in English](source.en.md)

# Сборка из исходных кодов

- [Требования](#требования)
- [Базовая инструкция](#базовая-инструкция)
- [Драйвер QEMU](#драйвер-qemu)

## Требования

- gcc и g++ >= 8, либо clang >= 10, либо другой компилятор с поддержкой C++11 плюс
  назначенных инициализаторов (designated initializers) из C++20
- CMake
- Заголовки и библиотеки jerasure
- Опционально - заголовки и библиотеки ISA-L, libibverbs, librdmacm
- tcmalloc (google-perftools-dev)

## Базовая инструкция

Скачайте исходные коды, например, из git: `git clone --recurse-submodules https://git.yourcmc.ru/vitalif/vitastor/`

Скачайте исходные коды пакета `fio`, распакуйте их и создайте символическую ссылку на них
в директории исходников Vitastor: `<vitastor>/fio`. Либо, если вы не хотите собирать плагин fio,
его можно исключить из сборки путём передачи `-DWITH_FIO=no` в cmake.

Собрать и установить Vitastor:

```
cd vitastor
mkdir build
cd build
cmake .. && make -j8 install
```

## Драйвер QEMU

Драйвер QEMU (qemu_driver.c) рекомендуется собирать вместе с самим QEMU. Для этого:
- Установите заголовки клиентской библиотеки Vitastor (из исходников или из пакета vitastor-client-dev)
- Возьмите соответствующий патч из `patches/qemu-*-vitastor.patch` и примените его к исходникам QEMU
- Скопируйте [src/client/qemu_driver.c](../../src/client/qemu_driver.c) в директорию исходников QEMU как `block/vitastor.c`
- Соберите QEMU как обычно

Однако в целях отладки драйвер также можно собирать отдельно от QEMU. Для этого:
- Установите QEMU, возьмите исходные коды установленного пакета, начните его пересборку,
  через некоторое время остановите её и скопируйте следующие заголовки:
   - `<qemu>/include` &rarr; `<vitastor>/qemu/include`
   - Debian:
      * Берите qemu из основного репозитория
      * `<qemu>/b/qemu/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * `<qemu>/b/qemu/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
   - CentOS 8:
      * Берите qemu из репозитория Advanced-Virtualization. Чтобы включить его, запустите
        `yum install centos-release-advanced-virtualization.noarch` и далее `yum install qemu`
      * `<qemu>/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * Для QEMU 3.0+: `<qemu>/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
      * Для QEMU 2.0+: `<qemu>/qapi-types.h` &rarr; `<vitastor>/qemu/b/qemu/qapi-types.h`
   - `config-host.h` и `qapi` нужны, т.к. в них содержатся автогенерируемые заголовки
- Сконфигурируйте cmake Vitastor с `WITH_QEMU=yes` (`cmake .. -DWITH_QEMU=yes`) и, если вы
  используете RHEL-подобный дистрибутив, также с `QEMU_PLUGINDIR=qemu-kvm`.
- После этого в процессе сборки Vitastor также будет собираться подходящий для вашей
  версии QEMU `block-vitastor.so`.
- Таким образом можно использовать драйвер даже с немодифицированным QEMU, но в этом случае
  диски Vitastor не будут работать через QAPI и через JSON-синтаксис `-blockdev`, а также
  потребуется устанавливать переменную окружения
  `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so`.
