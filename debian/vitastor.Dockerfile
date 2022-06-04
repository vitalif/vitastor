# Build Vitastor packages for Debian Buster or Bullseye/Sid inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/vitastor.Dockerfile .

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
RUN apt-get -y install fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep fio
RUN apt-get --download-only source fio
RUN apt-get update && apt-get -y install libjerasure-dev cmake libibverbs-dev libisal-dev

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
    cp -r /root/vitastor vitastor-0.7.1; \
    cd vitastor-0.7.1; \
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
    cd /root/packages/vitastor-$REL; \
    tar --sort=name --mtime='2020-01-01' --owner=0 --group=0 --exclude=debian -cJf vitastor_0.7.1.orig.tar.xz vitastor-0.7.1; \
    cd vitastor-0.7.1; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)\).*$/$1/'); \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v "$V""$REL" "Rebuild for $REL"; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/vitastor-$REL/vitastor-*/
