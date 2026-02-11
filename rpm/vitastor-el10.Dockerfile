# Build packages for AlmaLinux 10 inside a container
# cd ..
# docker pull --platform=linux/amd64/v2 quay.io/almalinuxorg/almalinux:10
# docker build -t vitastor-buildenv:el10 -f rpm/vitastor-el10.Dockerfile .
# docker run -i --rm -v ./:/root/vitastor vitastor-buildenv:el10 /root/vitastor/rpm/vitastor-build.sh

FROM quay.io/almalinuxorg/almalinux:10

WORKDIR /root

RUN sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/*.repo
RUN dnf -y install epel-release dnf-plugins-core
RUN dnf -y install https://vitastor.io/rpms/centos/10/vitastor-release-1.0-1.el10.noarch.rpm
RUN dnf -y install gcc-c++ gperftools-devel fio nodejs rpm-build jerasure-devel isa-l-devel gf-complete-devel rdma-core-devel cmake libnl3-devel
RUN dnf download --source fio
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --spec fio.spec
