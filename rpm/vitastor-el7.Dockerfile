# Build packages for CentOS 7 inside a container
# cd ..
# docker build -t vitastor-buildenv:el7 -f rpm/vitastor-el7.Dockerfile .
# docker run -i --rm -v ./:/root/vitastor vitastor-buildenv:el7 /root/vitastor/rpm/vitastor-build.sh

# localedef -i ru_RU -f UTF-8 ru_RU.UTF-8

FROM centos:7

WORKDIR /root

RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN sed -i 's/^mirrorlist=/#mirrorlist=/; s!#baseurl=http://mirror.centos.org/centos/\$releasever!baseurl=http://vault.centos.org/7.9.2009!' /etc/yum.repos.d/*.repo
RUN yum -y --enablerepo=extras install centos-release-scl epel-release yum-utils rpm-build
RUN perl -i -pe 's!mirrorlist=!#mirrorlist=!s; s!#\s*baseurl=http://mirror.centos.org!baseurl=http://vault.centos.org!' /etc/yum.repos.d/CentOS-SCLo-scl*.repo
RUN yum -y install https://vitastor.io/rpms/centos/7/vitastor-release-1.0-1.el7.noarch.rpm
RUN yum -y install devtoolset-9-gcc-c++ devtoolset-9-libatomic-devel gcc make cmake gperftools-devel \
    fio rh-nodejs12 jerasure-devel libisa-l-devel gf-complete-devel rdma-core-devel libnl3-devel
RUN yumdownloader --disablerepo=centos-sclo-rh --source fio
RUN rpm --nomd5 -i fio*.src.rpm
RUN rm -f /etc/yum.repos.d/CentOS-Media.repo
RUN cd ~/rpmbuild/SPECS && yum-builddep -y fio.spec
RUN yum -y install cmake3

ADD https://vitastor.io/rpms/liburing-el7/liburing-0.7-2.el7.src.rpm /root

RUN set -e; \
    rpm -i liburing*.src.rpm; \
    cd ~/rpmbuild/SPECS/; \
    . /opt/rh/devtoolset-9/enable; \
    rpmbuild -ba liburing.spec; \
    mkdir -p /root/packages/liburing-el7; \
    rm -rf /root/packages/liburing-el7/*; \
    cp ~/rpmbuild/RPMS/*/liburing* /root/packages/liburing-el7/; \
    cp ~/rpmbuild/SRPMS/liburing* /root/packages/liburing-el7/

RUN rpm -i `ls /root/packages/liburing-el7/liburing-*.x86_64.rpm | grep -v debug`
