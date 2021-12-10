#!/bin/bash
# QEMU patches don't include the `block/vitastor.c` file to not duplicate it in sources
# Run this script to append its creation to all QEMU patches

DIR=$(dirname $0)
for i in "$DIR"/qemu-*-vitastor.patch "$DIR"/pve-qemu-*-vitastor.patch; do
    if ! grep -qP '^\+\+\+ .*block/vitastor\.c' $i; then
        echo 'Index: a/block/vitastor.c' >> $i
        echo '===================================================================' >> $i
        echo '--- /dev/null' >> $i
        echo '+++ a/block/vitastor.c' >> $i
        echo '@@ -0,0 +1,'$(wc -l "$DIR"/../src/qemu_driver.c)' @@' >> $i
        cat "$DIR"/../src/qemu_driver.c | sed 's/^/+/' >> $i
    fi
done
