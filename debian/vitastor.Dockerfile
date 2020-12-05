# Build Vitastor packages for Debian Buster or Bullseye/Sid inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/build:/root/build -f debian/vitastor.Dockerfile .

ARG REL=bullseye

FROM debian:$REL

# again, it doesn't work otherwise
ARG REL=bullseye

WORKDIR /root

RUN if [ "$REL" = "buster" ]; then \
        echo 'deb http://deb.debian.org/debian buster-backports main' >> /etc/apt/sources.list; \
        echo >> /etc/apt/preferences; \
        echo 'Package: *' >> /etc/apt/preferences; \
        echo 'Pin: release a=buster-backports' >> /etc/apt/preferences; \
        echo 'Pin-Priority: 500' >> /etc/apt/preferences; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install qemu fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep qemu
RUN apt-get -y build-dep fio
RUN apt-get --download-only source qemu
RUN apt-get --download-only source fio
RUN apt-get -y install libjerasure-dev

ADD . /root/vitastor
RUN set -e -x; \
    mkdir -p /root/fio-build/; \
    cd /root/fio-build/; \
    rm -rf /root/fio-build/*; \
    dpkg-source -x /root/fio*.dsc; \
    cd /root/build/qemu-$REL/; \
    rm -rf qemu*/; \
    dpkg-source -x qemu*.dsc; \
    cd /root/build/qemu-$REL/qemu*/; \
    debian/rules b/configure-stamp; \
    cd b/qemu; \
    make -j8 qapi; \
    mkdir -p /root/build/vitastor-$REL; \
    rm -rf /root/build/vitastor-$REL/*; \
    cd /root/build/vitastor-$REL; \
    cp -r /root/vitastor vitastor-0.5.1; \
    ln -s /root/build/qemu-$REL/qemu-*/ vitastor-0.5.1/qemu; \
    ln -s /root/fio-build/fio-*/ vitastor-0.5.1/fio; \
    cd vitastor-0.5.1; \
    FIO=$(head -n1 fio/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    QEMU=$(head -n1 qemu/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    sh copy-qemu-includes.sh; \
    sh copy-fio-includes.sh; \
    rm qemu fio; \
    mkdir -p a b debian/patches; \
    mv qemu-copy b/qemu; \
    mv fio-copy b/fio; \
    diff -NaurpbB a b > debian/patches/qemu-fio-headers.patch || true; \
    echo qemu-fio-headers.patch >> debian/patches/series; \
    rm -rf a b; \
    rm -rf /root/build/qemu-$REL/qemu*/; \
    echo "dep:fio=$FIO" > debian/substvars; \
    echo "dep:qemu=$QEMU" >> debian/substvars; \
    cd /root/build/vitastor-$REL; \
    tar --sort=name --mtime='2020-01-01' --owner=0 --group=0 --exclude=debian -cJf vitastor_0.5.1.orig.tar.xz vitastor-0.5.1; \
    cd vitastor-0.5.1; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v "$V""$REL" "Rebuild for $REL"; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/build/vitastor-$REL/vitastor-*/
