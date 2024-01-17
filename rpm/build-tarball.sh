#!/bin/bash
# Vitastor depends on QEMU and/or FIO headers, but QEMU and FIO don't have -devel packages
# So we have to copy their headers into the source tarball

set -e

VITASTOR=$(dirname $0)
VITASTOR=$(realpath "$VITASTOR/..")

EL=$(rpm --eval '%dist')
if [ "$EL" = ".el8" ]; then
    # CentOS 8
    . /opt/rh/gcc-toolset-9/enable
elif [ "$EL" = ".el7" ]; then
    # CentOS 7
    . /opt/rh/devtoolset-9/enable
fi
cd ~/rpmbuild/SPECS
rpmbuild -bp fio.spec
cd $VITASTOR
ln -s ~/rpmbuild/BUILD/fio*/ fio
sh copy-fio-includes.sh
rm fio
mv fio-copy fio
FIO=`rpm -qi fio | perl -e 'while(<>) { /^Epoch[\s:]+(\S+)/ && print "$1:"; /^Version[\s:]+(\S+)/ && print $1; /^Release[\s:]+(\S+)/ && print "-$1"; }'`
perl -i -pe 's/(Requires:\s*fio)([^\n]+)?/$1 = '$FIO'/' $VITASTOR/rpm/vitastor-el$EL.spec
tar --transform 's#^#vitastor-1.4.1/#' --exclude 'rpm/*.rpm' -czf $VITASTOR/../vitastor-1.4.1$(rpm --eval '%dist').tar.gz *
