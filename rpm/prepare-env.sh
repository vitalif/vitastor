#!/bin/bash
# Install/get required packages on CentOS 7

set -e

rm -f /etc/yum.repos.d/CentOS-Media.repo
yum -y --enablerepo=extras install centos-release-scl epel-release yum-utils rpm-build qemu fio
yum -y --enablerepo='*' install devtoolset-9-gcc-c++ devtoolset-9-libatomic-devel gperftools-devel
yumdownloader --source qemu
yumdownloader --source fio
rpm --nomd5 -i qemu*.src.rpm
rpm --nomd5 -i fio*.src.rpm
cd ~/rpmbuild/SPECS
yum-builddep -y --enablerepo='*' qemu.spec
yum-builddep -y --enablerepo='*' fio.spec
