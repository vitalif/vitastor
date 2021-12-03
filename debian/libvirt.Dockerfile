# Build patched libvirt for Debian Buster or Bullseye/Sid inside a container
# cd ..; podman build --build-arg REL=bullseye -v `pwd`/packages:/root/packages -f debian/libvirt.Dockerfile .

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

RUN apt-get update; apt-get -y install devscripts
RUN apt-get -y build-dep libvirt0
RUN apt-get -y install libglusterfs-dev
RUN apt-get --download-only source libvirt

ADD patches/libvirt-5.0-vitastor.diff patches/libvirt-7.0-vitastor.diff patches/libvirt-7.5-vitastor.diff patches/libvirt-7.6-vitastor.diff /root
RUN set -e; \
    mkdir -p /root/packages/libvirt-$REL; \
    rm -rf /root/packages/libvirt-$REL/*; \
    cd /root/packages/libvirt-$REL; \
    dpkg-source -x /root/libvirt*.dsc; \
    D=$(ls -d libvirt-*/); \
    V=$(ls -d libvirt-*/ | perl -pe 's/libvirt-(\d+\.\d+).*/$1/'); \
    cp /root/libvirt-$V-vitastor.diff $D/debian/patches; \
    echo libvirt-$V-vitastor.diff >> $D/debian/patches/series; \
    cd $D; \
    V=$(head -n1 debian/changelog | perl -pe 's/^.*\((.*?)(~bpo[\d\+]*)?(\+deb[u\d]+)?\).*$/$1/')+vitastor2; \
    DEBEMAIL="Vitaliy Filippov <vitalif@yourcmc.ru>" dch -D $REL -v $V 'Add Vitastor support'; \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage --jobs=auto -sa; \
    rm -rf /root/packages/libvirt-$REL/$D
