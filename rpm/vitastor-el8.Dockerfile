# Build packages for CentOS 8 inside a container
# cd ..; podman build -t vitastor-el8 -v `pwd`/build:/root/build -f rpm/vitastor-el8.Dockerfile .

FROM centos:8

WORKDIR /root

RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN dnf -y install centos-release-advanced-virtualization epel-release dnf-plugins-core
RUN dnf --enablerepo='centos-advanced-virtualization' -y install gcc-toolset-9 gcc-toolset-9-gcc-c++ gperftools-devel qemu-kvm fio nodejs rpm-build
RUN rm -rf /var/lib/dnf/*; dnf download --disablerepo='*' --enablerepo='centos-advanced-virtualization-source' --source qemu-kvm
RUN dnf download --source fio
RUN rpm --nomd5 -i qemu*.src.rpm
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo='*' --spec qemu-kvm.spec
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo='*' --spec fio.spec

ADD https://vitastor.io/rpms/liburing-el7/liburing-0.7-2.el7.src.rpm /root

RUN set -e; \
    rpm -i liburing*.src.rpm; \
    cd ~/rpmbuild/SPECS/; \
    . /opt/rh/gcc-toolset-9/enable; \
    rpmbuild -ba liburing.spec; \
    mkdir -p /root/build/liburing-el8; \
    rm -rf /root/build/liburing-el8/*; \
    cp ~/rpmbuild/RPMS/*/liburing* /root/build/liburing-el8/; \
    cp ~/rpmbuild/SRPMS/liburing* /root/build/liburing-el8/

RUN rpm -i `ls /root/build/liburing-el7/liburing-*.x86_64.rpm | grep -v debug`

ADD . /root/vitastor

RUN set -e; \
    mkdir -p /root/build/qemu-el8; \
    rm -rf /root/build/qemu-el8/*; \
    rpm --nomd5 -i /root/qemu*.src.rpm; \
    cd ~/rpmbuild/SPECS; \
    PN=$(grep ^Patch qemu-kvm.spec | tail -n1 | perl -pe 's/Patch(\d+).*/$1/'); \
    csplit qemu-kvm.spec "/^Patch$PN/"; \
    cat xx00 > qemu-kvm.spec; \
    head -n 1 xx01 >> qemu-kvm.spec; \
    echo "Patch$((PN+1)): qemu-4.2-vitastor.patch" >> qemu-kvm.spec; \
    tail -n +2 xx01 >> qemu-kvm.spec; \
    perl -i -pe 's/(^Release:\s*\d+)/$1.vitastor/' qemu-kvm.spec; \
    cp /root/vitastor/qemu-4.2-vitastor.patch ~/rpmbuild/SOURCES; \
    rpmbuild --nocheck -ba qemu-kvm.spec; \
    cp ~/rpmbuild/RPMS/*/*qemu* /root/build/qemu-el8/; \
    cp ~/rpmbuild/SRPMS/*qemu* /root/build/qemu-el8/

RUN set -e; \
    cd /root/vitastor/rpm; \
    sh build-tarball.sh; \
    cp /root/vitastor-0.5.el8.tar.gz ~/rpmbuild/SOURCES; \
    cp vitastor-el8.spec ~/rpmbuild/SPECS/vitastor.spec; \
    cd ~/rpmbuild/SPECS/; \
    rpmbuild -ba vitastor.spec; \
    mkdir -p /root/build/vitastor-el8; \
    rm -rf /root/build/vitastor-el8/*; \
    cp ~/rpmbuild/RPMS/*/vitastor* /root/build/vitastor-el8/; \
    cp ~/rpmbuild/SRPMS/vitastor* /root/build/vitastor-el8/
