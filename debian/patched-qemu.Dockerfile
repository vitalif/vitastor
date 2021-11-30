# Build patched QEMU for Debian Buster or Bullseye/Sid inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/patched-qemu.Dockerfile .

ARG REL=
FROM debian:$REL
ARG REL=

WORKDIR /root

RUN if [ "$REL" = "buster" -o "$REL" = "bullseye" ]; then \
        echo "deb http://deb.debian.org/debian $REL-backports main" >> /etc/apt/sources.list; \
        echo >> /etc/apt/preferences; \
        echo 'Package: *' >> /etc/apt/preferences; \
        echo "Pin: release a=$REL-backports" >> /etc/apt/preferences; \
        echo 'Pin-Priority: 500' >> /etc/apt/preferences; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install qemu fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep qemu
# To build a custom version
#RUN cp /root/packages/qemu-orig/* /root
RUN apt-get --download-only source qemu

ADD patches/qemu-5.0-vitastor.patch patches/qemu-5.1-vitastor.patch patches/qemu-6.1-vitastor.patch src/qemu_driver.c /root/vitastor/patches/
RUN set -e; \
    apt-get install -y wget; \
    wget -q -O /etc/apt/trusted.gpg.d/vitastor.gpg https://vitastor.io/debian/pubkey.gpg; \
    (echo deb http://vitastor.io/debian $REL main > /etc/apt/sources.list.d/vitastor.list); \
    (echo "APT::Install-Recommends false;" > /etc/apt/apt.conf) && \
    apt-get update; \
    apt-get install -y vitastor-client vitastor-client-dev quilt; \
    mkdir -p /root/packages/qemu-$REL; \
    rm -rf /root/packages/qemu-$REL/*; \
    cd /root/packages/qemu-$REL; \
    dpkg-source -x /root/qemu*.dsc; \
    if ls -d /root/packages/qemu-$REL/qemu-5.0*; then \
        D=$(ls -d /root/packages/qemu-$REL/qemu-5.0*); \
        cp /root/vitastor/patches/qemu-5.0-vitastor.patch $D/debian/patches; \
        echo qemu-5.0-vitastor.patch >> $D/debian/patches/series; \
    elif ls /root/packages/qemu-$REL/qemu-6.1*; then \
        D=$(ls -d /root/packages/qemu-$REL/qemu-6.1*); \
        cp /root/vitastor/patches/qemu-6.1-vitastor.patch $D/debian/patches; \
        echo qemu-6.1-vitastor.patch >> $D/debian/patches/series; \
    else \
        cp /root/vitastor/patches/qemu-5.1-vitastor.patch /root/packages/qemu-$REL/qemu-*/debian/patches; \
        P=`ls -d /root/packages/qemu-$REL/qemu-*/debian/patches`; \
        echo qemu-5.1-vitastor.patch >> $P/series; \
    fi; \
    cd /root/packages/qemu-$REL/qemu-*/; \
    quilt push -a; \
    quilt add block/vitastor.c; \
    cp /root/vitastor/patches/qemu_driver.c block/vitastor.c; \
    quilt refresh; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)?\).*$/$1/')+vitastor1; \
    DEBEMAIL="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/qemu-$REL/qemu-*/
