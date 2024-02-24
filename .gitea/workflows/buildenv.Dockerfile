FROM node:16-bullseye

WORKDIR /root

ADD ./docker/vitastor.gpg /etc/apt/trusted.gpg.d

RUN echo 'deb http://deb.debian.org/debian bullseye-backports main' >> /etc/apt/sources.list; \
    echo 'deb http://vitastor.io/debian bullseye main' >> /etc/apt/sources.list; \
    echo >> /etc/apt/preferences; \
    echo 'Package: *' >> /etc/apt/preferences; \
    echo 'Pin: release a=bullseye-backports' >> /etc/apt/preferences; \
    echo 'Pin-Priority: 500' >> /etc/apt/preferences; \
    echo >> /etc/apt/preferences; \
    echo 'Package: *' >> /etc/apt/preferences; \
    echo 'Pin: origin "vitastor.io"' >> /etc/apt/preferences; \
    echo 'Pin-Priority: 1000' >> /etc/apt/preferences; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update
RUN apt-get -y install etcd qemu-system-x86 qemu-block-extra qemu-utils fio libasan5 \
    liburing1 liburing-dev libgoogle-perftools-dev devscripts libjerasure-dev cmake libibverbs-dev libisal-dev
RUN apt-get -y build-dep fio qemu=`dpkg -s qemu-system-x86|grep ^Version:|awk '{print $2}'`
RUN apt-get -y install jq lp-solve sudo nfs-common
RUN apt-get --download-only source fio qemu=`dpkg -s qemu-system-x86|grep ^Version:|awk '{print $2}'`

RUN set -ex; \
    mkdir qemu-build; \
    cd qemu-build; \
    dpkg-source -x /root/qemu*.dsc; \
    cd qemu*/; \
    debian/rules configure-qemu || debian/rules b/configure-stamp; \
    cd b/qemu; \
    make -j8 config-poison.h || true; \
    make -j8 qapi/qapi-builtin-types.h
