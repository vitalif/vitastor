# Build packages for CentOS 8 inside a container
# cd ..; podman build -t vitastor-el8 -v `pwd`/packages:/root/packages -f rpm/vitastor-el8.Dockerfile .

FROM centos:8

WORKDIR /root

RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN dnf -y install centos-release-advanced-virtualization epel-release dnf-plugins-core
RUN yum -y install https://vitastor.io/rpms/centos/8/vitastor-release-1.0-1.el8.noarch.rpm
RUN dnf --enablerepo='centos-advanced-virtualization' -y install gcc-toolset-9 gcc-toolset-9-gcc-c++ gperftools-devel qemu-kvm fio nodejs rpm-build jerasure-devel gf-complete-devel
RUN rm -rf /var/lib/dnf/*; dnf download --disablerepo='*' --enablerepo='vitastor' --source qemu-kvm
RUN dnf download --source fio
RUN rpm --nomd5 -i qemu*.src.rpm
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo=powertools --spec qemu-kvm.spec
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo=powertools --spec fio.spec && dnf install -y cmake

ADD https://vitastor.io/rpms/liburing-el7/liburing-0.7-2.el7.src.rpm /root

RUN set -e; \
    rpm -i liburing*.src.rpm; \
    cd ~/rpmbuild/SPECS/; \
    . /opt/rh/gcc-toolset-9/enable; \
    rpmbuild -ba liburing.spec; \
    mkdir -p /root/packages/liburing-el8; \
    rm -rf /root/packages/liburing-el8/*; \
    cp ~/rpmbuild/RPMS/*/liburing* /root/packages/liburing-el8/; \
    cp ~/rpmbuild/SRPMS/liburing* /root/packages/liburing-el8/

RUN rpm -i `ls /root/packages/liburing-el7/liburing-*.x86_64.rpm | grep -v debug`

ADD . /root/vitastor

RUN set -e; \
    cd /root/vitastor/rpm; \
    sh build-tarball.sh; \
    cp /root/vitastor-0.5.8.el8.tar.gz ~/rpmbuild/SOURCES; \
    cp vitastor-el8.spec ~/rpmbuild/SPECS/vitastor.spec; \
    cd ~/rpmbuild/SPECS/; \
    rpmbuild -ba vitastor.spec; \
    mkdir -p /root/packages/vitastor-el8; \
    rm -rf /root/packages/vitastor-el8/*; \
    cp ~/rpmbuild/RPMS/*/vitastor* /root/packages/vitastor-el8/; \
    cp ~/rpmbuild/SRPMS/vitastor* /root/packages/vitastor-el8/
