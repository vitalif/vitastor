# Build packages for Debian 10 inside a container
# cd ..; podman build -v `pwd`/qemu-build:/root/qemu-build -f debian/vitastor-buster.Dockerfile .

FROM debian:buster

WORKDIR /root

RUN echo 'deb http://deb.debian.org/debian buster-backports main' >> /etc/apt/sources.list; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' > /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -t buster-backports -y install qemu fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -t buster-backports -y build-dep qemu
RUN apt-get -y build-dep fio
RUN apt-get -t buster-backports --download-only source qemu-kvm
RUN apt-get --download-only source fio

ADD qemu-5.0-vitastor.patch qemu-5.1-vitastor.patch /root/vitastor/
RUN set -e; \
    mkdir -p /root/qemu-build; \
    rm -rf /root/qemu-build/*; \
    cd /root/qemu-build; \
    dpkg-source -x /root/qemu*.dsc; \
    if [ -d /root/qemu-build/qemu-5.0 ]; then \
        cp /root/vitastor/qemu-5.0-vitastor.patch /root/qemu-build/qemu-5.0/debian/patches; \
        echo qemu-5.0-vitastor.patch >> /root/qemu-build/qemu-5.0/debian/patches/series; \
    else \
        cp /root/vitastor/qemu-5.1-vitastor.patch /root/qemu-build/qemu-*/debian/patches; \
        echo qemu-5.1-vitastor.patch >> /root/qemu-build/qemu-*/debian/patches/series; \
    fi; \
    cd /root/qemu-build/qemu-*/; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)\).*$/$1/')~vitastor1; \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D buster -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto; \
    rm -rf /root/qemu-build/qemu-*/
