# Build packages for Debian Bullseye/Sid inside a container
# cd ..; podman build -t vitastor-bullseye -v `pwd`/build:/root/build -f debian/vitastor-bullseye.Dockerfile .

ARG REL=bullseye

FROM debian:$REL

# again, it doesn't work otherwise
ARG REL=bullseye

WORKDIR /root

RUN grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' > /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install qemu fio liburing1 liburing-dev libgoogle-perftools-dev devscripts
RUN apt-get -y build-dep qemu
RUN apt-get -y build-dep fio
RUN apt-get --download-only source qemu
RUN apt-get --download-only source fio

ADD qemu-5.0-vitastor.patch qemu-5.1-vitastor.patch /root/vitastor/
RUN set -e; \
    mkdir -p /root/build/qemu-$REL; \
    rm -rf /root/build/qemu-$REL/*; \
    cd /root/build/qemu-$REL; \
    dpkg-source -x /root/qemu*.dsc; \
    if [ -d /root/build/qemu-$REL/qemu-5.0 ]; then \
        cp /root/vitastor/qemu-5.0-vitastor.patch /root/build/qemu-$REL/qemu-5.0/debian/patches; \
        echo qemu-5.0-vitastor.patch >> /root/build/qemu-$REL/qemu-5.0/debian/patches/series; \
    else \
        cp /root/vitastor/qemu-5.1-vitastor.patch /root/build/qemu-$REL/qemu-*/debian/patches; \
        P=`ls -d /root/build/qemu-$REL/qemu-*/debian/patches`; \
        echo qemu-5.1-vitastor.patch >> $P/series; \
    fi; \
    cd /root/build/qemu-$REL/qemu-*/; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)?\).*$/$1/')+vitastor1; \
    echo ">>> VERSION: $V"; \
    DEBFULLNAME="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v $V 'Plug Vitastor block driver'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto; \
    rm -rf /root/build/qemu-$REL/qemu-*/

RUN cd /root/build/qemu-$REL && apt-get -y install ./qemu-system-data*.deb ./qemu-system-common_*.deb ./qemu-system-x86_*.deb ./qemu_*.deb

ADD . /root/vitastor
RUN set -e; \
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
    cp -r /root/vitastor vitastor-0.5; \
    ln -s /root/build/qemu-$REL/qemu-*/ vitastor-0.5/qemu; \
    ln -s /root/fio-build/fio-*/ vitastor-0.5/fio; \
    cd vitastor-0.5; \
    sh copy-qemu-includes.sh; \
    sh copy-fio-includes.sh; \
    rm qemu fio; \
    mv qemu-copy qemu; \
    mv fio-copy fio; \
    rm -rf /root/build/qemu-$REL/qemu*/; \
    cd /root/build/vitastor-$REL; \
    tar -czf vitastor_0.5.orig.tar.gz vitastor-0.5; \
    cd vitastor-0.5; \
    ls -l; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto; \
    rm -rf /root/build/vitastor-$REL/vitastor-*/
