# Build environment for building Vitastor packages for Debian inside a container
# cd ..
# docker build --build-arg DISTRO=debian --build-arg REL=bullseye -f debian/vitastor.Dockerfile -t vitastor-buildenv:bullseye .
# docker run --rm -e REL=bullseye -v ./:/root/vitastor /root/vitastor/debian/vitastor-build.sh

ARG DISTRO=debian
ARG REL=
FROM $DISTRO:$REL
ARG DISTRO=debian
ARG REL=

WORKDIR /root

RUN set -e -x; \
    if [ "$REL" = "buster" ]; then \
        perl -i -pe 's/deb.debian.org/archive.debian.org/' /etc/apt/sources.list; \
        apt-get update; \
        apt-get -y install wget; \
        wget https://vitastor.io/debian/pubkey.gpg -O /etc/apt/trusted.gpg.d/vitastor.gpg; \
        echo "deb https://vitastor.io/debian $REL main" >> /etc/apt/sources.list; \
    fi; \
    grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb/deb-src/' >> /etc/apt/sources.list; \
    perl -i -pe 's/Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/*.sources || true; \
    echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests false;' >> /etc/apt/apt.conf

RUN apt-get update && \
    apt-get -y install fio libgoogle-perftools-dev devscripts libjerasure-dev cmake \
        libibverbs-dev librdmacm-dev libisal-dev libnl-3-dev libnl-genl-3-dev curl nodejs npm node-nan node-bindings && \
    apt-get -y build-dep fio && \
    apt-get --download-only source fio
