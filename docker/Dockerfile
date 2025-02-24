# Build Docker image with Vitastor packages

FROM debian:bookworm

ADD etc/apt /etc/apt/
RUN apt-get update && apt-get -y install vitastor qemu-system-x86 qemu-system-common qemu-block-extra qemu-utils jq nfs-common && apt-get clean
ADD sleep.sh /usr/bin/
ADD install.sh /usr/bin/
ADD scripts /opt/scripts/
ADD etc /etc/
RUN ln -s /usr/lib/vitastor/mon/make-etcd /usr/bin/make-etcd
