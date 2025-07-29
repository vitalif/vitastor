#!/bin/bash
# To be ran inside buildenv docker

set -e -x

[ -e /usr/lib/x86_64-linux-gnu/pkgconfig/libisal.pc ] || cp /root/vitastor/debian/libisal.pc /usr/lib/x86_64-linux-gnu/pkgconfig

mkdir -p /root/fio-build/
cd /root/fio-build/
rm -rf /root/fio-build/*
dpkg-source -x /root/fio*.dsc

FULLVER=`head -n1 /root/vitastor/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'`
VER=${FULLVER%%-*}
rm -rf /root/vitastor-$VER
mkdir /root/vitastor-$VER
cd /root/vitastor
cp -a $(ls | grep -v packages) /root/vitastor-$VER

rm -rf /root/vitastor/packages/vitastor-$REL
mkdir -p /root/vitastor/packages/vitastor-$REL
mv /root/vitastor-$VER /root/vitastor/packages/vitastor-$REL/

cd /root/vitastor/packages/vitastor-$REL/vitastor-$VER

rm -rf fio
ln -s /root/fio-build/fio-*/ ./fio
FIO=`head -n1 fio/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'`
ls /usr/include/linux/raw.h || cp ./debian/raw.h /usr/include/linux/raw.h
sh copy-fio-includes.sh
rm fio
mkdir -p a b debian/patches
mv fio-copy b/fio
diff -NaurpbB a b > debian/patches/fio-headers.patch || true
echo fio-headers.patch >> debian/patches/series
rm -rf a b

echo "dep:fio=$FIO" > debian/fio_version

cd /root/vitastor/packages/vitastor-$REL/vitastor-$VER
mkdir mon/node_modules
cd mon/node_modules
curl -s https://git.yourcmc.ru/vitalif/antietcd/archive/master.tar.gz | tar -zx
curl -s https://git.yourcmc.ru/vitalif/tinyraft/archive/master.tar.gz | tar -zx

cd /root/vitastor/packages/vitastor-$REL
tar --sort=name --mtime='2020-01-01' --owner=0 --group=0 --exclude=debian -cJf vitastor_$VER.orig.tar.xz vitastor-$VER
cd vitastor-$VER
DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v "$FULLVER""$REL" "Rebuild for $REL"
DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa
rm -rf /root/vitastor/packages/vitastor-$REL/vitastor-*/

# Why does ubuntu rename debug packages to *.ddeb?
if ls *.ddeb >/dev/null; then
    perl -i -pe 's/\.ddeb/.deb/' *.buildinfo *.changes
    for i in *.ddeb; do
        mv $i ${i%%.ddeb}.deb
    done
fi
