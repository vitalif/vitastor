[Documentation](../../README.md#documentation) → Installation → Building from Source

-----

[Читать на русском](source.ru.md)

# Building from Source

- [Requirements](#requirements)
- [Basic instructions](#basic-instructions)
- [QEMU Driver](#qemu-driver)

## Requirements

- gcc and g++ 8 or newer, clang 10 or newer, or other compiler with C++11 plus
  designated initializers support from C++20
- CMake
- liburing, jerasure headers and libraries
- ISA-L, libibverbs headers and libraries (optional)
- tcmalloc (google-perftools-dev)

## Basic instructions

Download source, for example using git: `git clone --recurse-submodules https://git.yourcmc.ru/vitalif/vitastor/`

Get `fio` source and symlink it into `<vitastor>/fio`. If you don't want to build fio engine,
you can disable it by passing `-DWITH_FIO=no` to cmake.

Build and install Vitastor:

```
cd vitastor
mkdir build
cd build
cmake .. && make -j8 install
```

## QEMU Driver

It's recommended to build the QEMU driver (qemu_driver.c) in-tree, as a part of
QEMU build process. To do that:
- Install vitastor client library headers (from source or from vitastor-client-dev package)
- Take a corresponding patch from `patches/qemu-*-vitastor.patch` and apply it to QEMU source
- Copy `src/client/qemu_driver.c` to QEMU source directory as `block/vitastor.c`
- Build QEMU as usual

But it is also possible to build it out-of-tree. To do that:
- Get QEMU source, begin to build it, stop the build and copy headers:
   - `<qemu>/include` &rarr; `<vitastor>/qemu/include`
   - Debian:
      * Use qemu packages from the main repository
      * `<qemu>/b/qemu/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * `<qemu>/b/qemu/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
   - CentOS 8:
      * Use qemu packages from the Advanced-Virtualization repository. To enable it, run
        `yum install centos-release-advanced-virtualization.noarch` and then `yum install qemu`
      * `<qemu>/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * For QEMU 3.0+: `<qemu>/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
      * For QEMU 2.0+: `<qemu>/qapi-types.h` &rarr; `<vitastor>/qemu/b/qemu/qapi-types.h`
   - `config-host.h` and `qapi` are required because they contain generated headers
- Configure Vitastor with `WITH_QEMU=yes` and, if you're on RHEL, also with `QEMU_PLUGINDIR=qemu-kvm`:
  `cmake .. -DWITH_QEMU=yes`.
- After that, Vitastor will build `block-vitastor.so` during its build process.
- This way you can use the driver even with unmodified QEMU, but you'll need to set
  environment variable `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so`
  and Vitastor disks won't work in QAPI and in "new" JSON syntax `-blockdev` in this case.
