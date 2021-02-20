#!/bin/bash

gcc -I. -E -o fio_headers.i src/fio_headers.h

rm -rf fio-copy
for i in `grep -Po 'fio/[^"]+' fio_headers.i | sort | uniq`; do
    j=${i##fio/}
    p=$(dirname $j)
    mkdir -p fio-copy/$p
    cp $i fio-copy/$j
done

rm fio_headers.i
