# Build packages for AlmaLinux 9 inside a container
# cd ..
# docker build -t vitastor-buildenv:el9 -f rpm/vitastor-el9.Dockerfile .
# docker run -i --rm -v ./:/root/vitastor vitastor-buildenv:el9 /root/vitastor/rpm/vitastor-build.sh

FROM almalinux:9

WORKDIR /root

RUN sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/*.repo
RUN dnf -y install epel-release dnf-plugins-core
RUN dnf -y install https://vitastor.io/rpms/centos/9/vitastor-release-1.0-1.el9.noarch.rpm
RUN dnf -y install gcc-c++ gperftools-devel fio nodejs rpm-build jerasure-devel libisa-l-devel gf-complete-devel rdma-core-devel libarchive cmake libnl3-devel
RUN dnf download --source fio
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --spec fio.spec
