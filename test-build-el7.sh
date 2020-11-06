#!/bin/bash

# Cheatsheet for CentOS 7 packaging (not a build script)

set -e
rm -f /etc/yum.repos.d/CentOS-Media.repo
yum -y --enablerepo=extras install centos-release-scl epel-release
yum -y --enablerepo='*' install devtoolset-9-gcc-c++ devtoolset-9-libatomic-devel gperftools-devel
yumdownloader --source qemu
yumdownloader --source fio
yum-builddep -y --enablerepo='*' qemu
yum -y install rpm-build
. /opt/rh/devtoolset-9/enable
rpm --nomd5 -i qemu*.src.rpm
rpm --nomd5 -i fio*.src.rpm
cd ~/rpmbuild/SPECS
rpmbuild -bp fio.spec
perl -i -pe 's/^make V=1/exit 1; make V=1/' qemu.spec
rpmbuild -bc qemu.spec
perl -i -pe 's/^exit 1; make V=1/make V=1/' qemu.spec
cd ~/rpmbuild/BUILD/qemu*/
make qapi-types.h
mkdir -p ~/vitastor/qemu/b/qemu
cp config-host.h ~/vitastor/qemu/b/qemu 
cp qapi-types.h ~/vitastor/qemu/b/qemu
cp -r include ~/vitastor/qemu
cd ~/vitastor
sh copy-qemu-includes.sh
mv qemu qemu-old
mv qemu-copy qemu
ln -s ~/rpmbuild/BUILD/fio*/ fio
sh copy-fio-includes.sh
rm fio
mv fio-copy fio
