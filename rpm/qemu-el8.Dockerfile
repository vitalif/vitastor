# Build packages for CentOS 8 inside a container
# cd ..; podman build -t qemu-el8 -v `pwd`/build:/root/build -f rpm/qemu-el8.Dockerfile .

FROM centos:8

WORKDIR /root

RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN dnf -y install centos-release-advanced-virtualization epel-release dnf-plugins-core rpm-build
RUN rm -rf /var/lib/dnf/*; dnf download --disablerepo='*' --enablerepo='centos-advanced-virtualization-source' --source qemu-kvm
RUN rpm --nomd5 -i qemu*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo=PowerTools --spec qemu-kvm.spec

ADD qemu-*-vitastor.patch /root/vitastor/

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
