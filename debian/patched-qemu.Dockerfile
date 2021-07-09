# Build patched QEMU for Debian Buster or Bullseye/Sid inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/patched-qemu.Dockerfile .

FROM debian:$REL

WORKDIR /root

RUN if [ "$REL" = "buster" ]; then \
        echo 'deb http://deb.debian.org/debian buster-backports main' >> /etc/apt/sources.list; \
        echo >> /etc/apt/preferences; \
        echo 'Package: *' >> /etc/apt/preferences; \
        echo 'Pin: release a=buster-backports' >> /etc/apt/preferences; \
        echo 'Pin-Priority: 500' >> /etc/apt/preferences; \
        echo >> /etc/apt/preferences; \
        echo 'Package: libglvnd* libgles* libglx* libgl1 libegl* libopengl* mesa*' >> /etc/apt/preferences; \
        echo 'Pin: release a=buster-backports' >> /etc/apt/preferences; \
        echo 'Pin-Priority: 50' >> /etc/apt/preferences; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install qemu fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep qemu
RUN apt-get -y build-dep fio
# To build a custom version
#RUN cp /root/packages/qemu-orig/* /root
RUN apt-get --download-only source qemu
RUN apt-get --download-only source fio

ADD qemu-5.0-vitastor.patch qemu-5.1-vitastor.patch /root/vitastor/
RUN set -e; \
    mkdir -p /root/packages/qemu-$REL; \
    rm -rf /root/packages/qemu-$REL/*; \
    cd /root/packages/qemu-$REL; \
    dpkg-source -x /root/qemu*.dsc; \
    if [ -d /root/packages/qemu-$REL/qemu-5.0 ]; then \
        cp /root/vitastor/qemu-5.0-vitastor.patch /root/packages/qemu-$REL/qemu-5.0/debian/patches; \
        echo qemu-5.0-vitastor.patch >> /root/packages/qemu-$REL/qemu-5.0/debian/patches/series; \
    else \
        cp /root/vitastor/qemu-5.1-vitastor.patch /root/packages/qemu-$REL/qemu-*/debian/patches; \
        P=`ls -d /root/packages/qemu-$REL/qemu-*/debian/patches`; \
        echo qemu-5.1-vitastor.patch >> $P/series; \
    fi; \
    cd /root/packages/qemu-$REL/qemu-*/; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)?\).*$/$1/')+vitastor1; \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/qemu-$REL/qemu-*/
