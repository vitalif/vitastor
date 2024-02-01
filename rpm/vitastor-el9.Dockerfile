# Build packages for AlmaLinux 9 inside a container
# cd ..; podman build -t vitastor-el9 -v `pwd`/packages:/root/packages -f rpm/vitastor-el9.Dockerfile .

FROM almalinux:9

WORKDIR /root

RUN sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/*.repo
RUN dnf -y install epel-release dnf-plugins-core
RUN dnf -y install https://vitastor.io/rpms/centos/9/vitastor-release-1.0-1.el9.noarch.rpm
RUN dnf -y install gcc-c++ gperftools-devel fio nodejs rpm-build jerasure-devel libisa-l-devel gf-complete-devel rdma-core-devel libarchive liburing-devel cmake libnl3-devel
RUN dnf download --source fio
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --spec fio.spec

ADD . /root/vitastor

RUN set -e; \
    cd /root/vitastor/rpm; \
    sh build-tarball.sh; \
    cp /root/vitastor-1.4.1.el9.tar.gz ~/rpmbuild/SOURCES; \
    cp vitastor-el9.spec ~/rpmbuild/SPECS/vitastor.spec; \
    cd ~/rpmbuild/SPECS/; \
    rpmbuild -ba vitastor.spec; \
    mkdir -p /root/packages/vitastor-el9; \
    rm -rf /root/packages/vitastor-el9/*; \
    cp ~/rpmbuild/RPMS/*/vitastor* /root/packages/vitastor-el9/; \
    cp ~/rpmbuild/SRPMS/vitastor* /root/packages/vitastor-el9/
