# Build Vitastor packages for Debian inside a container
# cd ..; podman build --build-arg DISTRO=debian --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/vitastor.Dockerfile .

ARG DISTRO=debian
ARG REL=
FROM $DISTRO:$REL
ARG DISTRO=debian
ARG REL=

WORKDIR /root

RUN set -e -x; \
    if [ "$REL" = "buster" ]; then \
        apt-get update; \
        apt-get -y install wget; \
        wget https://vitastor.io/debian/pubkey.gpg -O /etc/apt/trusted.gpg.d/vitastor.gpg; \
        echo "deb https://vitastor.io/debian $REL main" >> /etc/apt/sources.list; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    perl -i -pe 's/Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources || true; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update && \
    apt-get -y install fio liburing-dev libgoogle-perftools-dev devscripts libjerasure-dev cmake libibverbs-dev librdmacm-dev libisal-dev libnl-3-dev libnl-genl-3-dev curl && \
    apt-get -y build-dep fio && \
    apt-get --download-only source fio

ADD . /root/vitastor
RUN set -e -x; \
    [ -e /usr/lib/x86_64-linux-gnu/pkgconfig/libisal.pc ] || cp /root/vitastor/debian/libisal.pc /usr/lib/x86_64-linux-gnu/pkgconfig; \
    mkdir -p /root/fio-build/; \
    cd /root/fio-build/; \
    rm -rf /root/fio-build/*; \
    dpkg-source -x /root/fio*.dsc; \
    mkdir -p /root/packages/vitastor-$REL; \
    rm -rf /root/packages/vitastor-$REL/*; \
    cd /root/packages/vitastor-$REL; \
    FULLVER=$(head -n1 /root/vitastor/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    VER=${FULLVER%%-*}; \
    cp -r /root/vitastor vitastor-$VER; \
    cd vitastor-$VER; \
    ln -s /root/fio-build/fio-*/ ./fio; \
    FIO=$(head -n1 fio/debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    ls /usr/include/linux/raw.h || cp ./debian/raw.h /usr/include/linux/raw.h; \
    sh copy-fio-includes.sh; \
    rm fio; \
    mkdir -p a b debian/patches; \
    mv fio-copy b/fio; \
    diff -NaurpbB a b > debian/patches/fio-headers.patch || true; \
    echo fio-headers.patch >> debian/patches/series; \
    rm -rf a b; \
    echo "dep:fio=$FIO" > debian/fio_version; \
    cd /root/packages/vitastor-$REL/vitastor-$VER; \
    mkdir mon/node_modules; \
    cd mon/node_modules; \
    curl -s https://git.yourcmc.ru/vitalif/antietcd/archive/master.tar.gz | tar -zx; \
    curl -s https://git.yourcmc.ru/vitalif/tinyraft/archive/master.tar.gz | tar -zx; \
    cd /root/packages/vitastor-$REL; \
    tar --sort=name --mtime='2020-01-01' --owner=0 --group=0 --exclude=debian -cJf vitastor_$VER.orig.tar.xz vitastor-$VER; \
    cd vitastor-$VER; \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v "$FULLVER""$REL" "Rebuild for $REL"; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/vitastor-$REL/vitastor-*/
