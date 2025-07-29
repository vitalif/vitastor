#!/bin/bash

set -e -x
REL=$(rpm --eval '%dist')
REL=${REL##.}
cd /root/vitastor/rpm
./build-tarball.sh
VER=$(grep ^Version: vitastor-$REL.spec | awk '{print $2}')
cp /root/vitastor-$VER.$REL.tar.gz ~/rpmbuild/SOURCES
cp vitastor-$REL.spec ~/rpmbuild/SPECS/vitastor.spec
cd ~/rpmbuild/SPECS/
rpmbuild -ba vitastor.spec
mkdir -p /root/vitastor/packages/vitastor-$REL
rm -rf /root/vitastor/packages/vitastor-$REL/*
cp ~/rpmbuild/RPMS/*/*vitastor* /root/vitastor/packages/vitastor-$REL/
cp ~/rpmbuild/SRPMS/vitastor* /root/vitastor/packages/vitastor-$REL/
