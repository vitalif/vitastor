# Build packages for Debian 10 inside a container
# cd ..; podman build -t vitastor-buster -v `pwd`/build:/root/build -f debian/vitastor-buster.Dockerfile .

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
    mkdir -p /root/build/qemu-buster; \
    rm -rf /root/build/qemu-buster/*; \
    cd /root/build/qemu-buster; \
    dpkg-source -x /root/qemu*.dsc; \
    if [ -d /root/build/qemu-buster/qemu-5.0 ]; then \
        cp /root/vitastor/qemu-5.0-vitastor.patch /root/build/qemu-buster/qemu-5.0/debian/patches; \
        echo qemu-5.0-vitastor.patch >> /root/build/qemu-buster/qemu-5.0/debian/patches/series; \
    else \
        cp /root/vitastor/qemu-5.1-vitastor.patch /root/build/qemu-buster/qemu-*/debian/patches; \
        echo qemu-5.1-vitastor.patch >> /root/build/qemu-buster/qemu-*/debian/patches/series; \
    fi; \
    cd /root/build/qemu-buster/qemu-*/; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)\).*$/$1/')~vitastor1; \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D buster -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto; \
    rm -rf /root/build/qemu-buster/qemu-*/

RUN cd /root/build/qemu-buster && apt-get -y -t buster-backports install ./qemu-system-data*.deb ./qemu-system-common_*.deb ./qemu-system-x86_*.deb ./qemu_*.deb

ADD . /root/vitastor
RUN set -e; \
    mkdir -p /root/fio-build/; \
    cd /root/fio-build/; \
    rm -rf /root/fio-build/*; \
    dpkg-source -x /root/fio*.dsc; \
    cd /root/build/qemu-buster/; \
    rm -rf qemu*/; \
    dpkg-source -x qemu*.dsc; \
    cd /root/build/qemu-buster/qemu*/; \
    debian/rules b/configure-stamp; \
    cd b/qemu; \
    make -j8 qapi; \
    mkdir -p /root/build/vitastor-buster; \
    rm -rf /root/build/vitastor-buster/*; \
    cd /root/build/vitastor-buster; \
    cp -r /root/vitastor vitastor-0.5; \
    ln -s /root/build/qemu-buster/qemu-*/ vitastor-0.5/qemu; \
    ln -s /root/fio-build/fio-*/ vitastor-0.5/fio; \
    cd vitastor-0.5; \
    sh copy-qemu-includes.sh; \
    sh copy-fio-includes.sh; \
    rm qemu fio; \
    mv qemu-copy qemu; \
    mv fio-copy fio; \
    rm -rf /root/build/qemu-buster/qemu*/; \
    cd /root/build/vitastor-buster; \
    tar -czf vitastor_0.5.orig.tar.gz vitastor-0.5; \
    cd vitastor-0.5; \
    ls -l; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto; \
    rm -rf /root/build/vitastor-buster/vitastor-*/
