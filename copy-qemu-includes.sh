#!/bin/bash

#cd qemu
#debian/rules b/configure-stamp
#cd b/qemu; make qapi

gcc -I qemu/b/qemu `pkg-config glib-2.0 --cflags` \
    -I qemu/include -E -o qemu_driver.i qemu_driver.c

rm -rf qemu-copy
for i in `grep -Po 'qemu/[^"]+' qemu_driver.i | sort | uniq`; do
    j=${i##qemu/}
    p=$(dirname $j)
    mkdir -p qemu-copy/$p
    cp $i qemu-copy/$j
done

rm qemu_driver.i
