#!/bin/bash
# Vitastor depends on QEMU and FIO headers, but QEMU and FIO don't have -devel packages
# So we have to copy their headers into the source tarball

set -e

VITASTOR=$(dirname $0)
VITASTOR=$(realpath "$VITASTOR/..")

. /opt/rh/devtoolset-9/enable
cd ~/rpmbuild/SPECS
rpmbuild -bp fio.spec
perl -i -pe 's/^make V=1/exit 0; make V=1/' qemu.spec
rpmbuild -bc qemu.spec
perl -i -pe 's/^exit 0; make V=1/make V=1/' qemu.spec
cd ~/rpmbuild/BUILD/qemu*/
rm -rf $VITASTOR/qemu $VITASTOR/fio
mkdir -p $VITASTOR/qemu/b/qemu
make config-host.h
cp config-host.h $VITASTOR/qemu/b/qemu
cp -r include $VITASTOR/qemu
if [ -f qapi-schema.json ]; then
    # QEMU 2.0
    make qapi-types.h
    cp qapi-types.h $VITASTOR/qemu/b/qemu
else
    # QEMU 3.0+
    make qapi
    cp -r qapi $VITASTOR/qemu/b/qemu
fi
cd $VITASTOR
sh copy-qemu-includes.sh
rm -rf qemu
mv qemu-copy qemu
ln -s ~/rpmbuild/BUILD/fio*/ fio
sh copy-fio-includes.sh
rm fio
mv fio-copy fio
FIO=`rpm -qa fio | perl -pe 's/^fio-(.*)\.[^\.]+$/$1/'`
QEMU=`rpm -qa qemu | perl -pe 's/^qemu-(.*)\.[^\.]+$/$1/'`
perl -i -pe 's/(Requires:\s*fio)([^\n]+)?/$1 = '$FIO'/' $VITASTOR/rpm/vitastor.spec
perl -i -pe 's/(Requires:\s*qemu)([^\n]+)?/$1 = '$QEMU'/' $VITASTOR/rpm/vitastor.spec
tar --transform 's#^#vitastor-0.5/#' --exclude 'rpm/*.rpm' -czf $VITASTOR/../vitastor-0.5$(rpm --eval '%dist').tar.gz *
