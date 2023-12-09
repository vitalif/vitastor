# Build patched QEMU for Debian inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/patched-qemu.Dockerfile .

ARG REL=
FROM debian:$REL
ARG REL=

WORKDIR /root

RUN if [ "$REL" = "buster" -o "$REL" = "bullseye" -o "$REL" = "bookworm" ]; then \
        echo "deb http://deb.debian.org/debian $REL-backports main" >> /etc/apt/sources.list; \
        echo >> /etc/apt/preferences; \
        echo 'Package: *' >> /etc/apt/preferences; \
        echo "Pin: release a=$REL-backports" >> /etc/apt/preferences; \
        echo 'Pin-Priority: 500' >> /etc/apt/preferences; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    perl -i -pe 's/Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources || true; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install fio liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep qemu
# To build a custom version
#RUN cp /root/packages/qemu-orig/* /root
RUN apt-get --download-only source qemu

ADD patches /root/vitastor/patches
ADD src/qemu_driver.c /root/vitastor/src/qemu_driver.c

#RUN set -e; \
#    apt-get install -y wget; \
#    wget -q -O /etc/apt/trusted.gpg.d/vitastor.gpg https://vitastor.io/debian/pubkey.gpg; \
#    (echo deb http://vitastor.io/debian $REL main > /etc/apt/sources.list.d/vitastor.list); \
#    (echo "APT::Install-Recommends false;" > /etc/apt/apt.conf) && \
#    apt-get update; \
#    apt-get install -y vitastor-client vitastor-client-dev quilt

RUN set -e; \
    dpkg -i /root/packages/vitastor-$REL/vitastor-client_*.deb /root/packages/vitastor-$REL/vitastor-client-dev_*.deb; \
    apt-get update; \
    apt-get install -y quilt; \
    mkdir -p /root/packages/qemu-$REL; \
    rm -rf /root/packages/qemu-$REL/*; \
    cd /root/packages/qemu-$REL; \
    dpkg-source -x /root/qemu*.dsc; \
    QEMU_VER=$(ls -d qemu*/ | perl -pe 's!^.*?(\d+\.\d+).*!$1!'); \
    D=$(ls -d qemu*/); \
    cp /root/vitastor/patches/qemu-$QEMU_VER-vitastor.patch ./qemu-*/debian/patches; \
    echo qemu-$QEMU_VER-vitastor.patch >> $D/debian/patches/series; \
    cd /root/packages/qemu-$REL/qemu-*/; \
    quilt push -a; \
    quilt add block/vitastor.c; \
    cp /root/vitastor/src/qemu_driver.c block/vitastor.c; \
    quilt refresh; \
    V=$(head -n1 debian/changelog | perl -pe 's/5\.2\+dfsg-9/5.2+dfsg-11/; s/^.*\((.*?)(~bpo[\d\+]*)?\).*$/$1/')+vitastor4; \
    if [ "$REL" = bullseye ]; then V=${V}bullseye; fi; \
    DEBEMAIL="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/qemu-$REL/qemu-*/
