# Build packages for CentOS 8 inside a container
# cd ..
# docker build -t vitastor-buildenv:el8 -f rpm/vitastor-el8.Dockerfile .
# docker run -i --rm -v ./:/root/vitastor vitastor-buildenv:el8 /root/vitastor/rpm/vitastor-build.sh

FROM centos:8

WORKDIR /root

RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN sed -i 's/^mirrorlist=/#mirrorlist=/; s!#baseurl=http://mirror.centos.org/!baseurl=http://vault.centos.org/!' /etc/yum.repos.d/*.repo
RUN dnf -y install centos-release-advanced-virtualization epel-release dnf-plugins-core
RUN sed -i 's/^mirrorlist=/#mirrorlist=/; s!#baseurl=.*!baseurl=http://vault.centos.org/centos/8.4.2105/virt/$basearch/$avdir/!; s!^baseurl=.*Source/.*!baseurl=http://vault.centos.org/centos/8.4.2105/virt/Source/advanced-virtualization/!' /etc/yum.repos.d/CentOS-Advanced-Virtualization.repo
RUN yum -y install https://vitastor.io/rpms/centos/8/vitastor-release-1.0-1.el8.noarch.rpm
RUN dnf -y install gcc-toolset-9 gcc-toolset-9-gcc-c++ gperftools-devel \
    fio nodejs rpm-build jerasure-devel libisa-l-devel gf-complete-devel libibverbs-devel libarchive cmake libnl3-devel
RUN dnf download --source fio
RUN rpm --nomd5 -i fio*.src.rpm
RUN cd ~/rpmbuild/SPECS && dnf builddep -y --enablerepo=powertools --spec fio.spec

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

RUN rpm -i `ls /root/packages/liburing-el8/liburing-*.x86_64.rpm | grep -v debug`
