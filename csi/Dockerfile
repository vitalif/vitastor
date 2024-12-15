# Compile stage
FROM golang:bookworm AS build

ADD go.sum go.mod /app/
RUN cd /app; CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go mod download -x
ADD . /app
RUN perl -i -e '$/ = undef; while(<>) { s/\n\s*(\{\s*\n)/$1\n/g; s/\}(\s*\n\s*)else\b/$1} else/g; print; }' `find /app -name '*.go'` && \
    cd /app && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o vitastor-csi

# Final stage
FROM debian:bookworm

LABEL maintainers="Vitaliy Filippov <vitalif@yourcmc.ru>"
LABEL description="Vitastor CSI Driver"

ENV NODE_ID=""
ENV CSI_ENDPOINT=""

RUN apt-get update && \
    apt-get install -y wget && \
    (echo "APT::Install-Recommends false;" > /etc/apt/apt.conf) && \
    apt-get update && \
    apt-get install -y e2fsprogs xfsprogs kmod iproute2 \
        # NFS mount dependencies
        nfs-common netbase \
        # dependencies of qemu-storage-daemon
        libnuma1 liburing2 libglib2.0-0 libfuse3-3 libaio1 libzstd1 libnettle8 \
        libgmp10 libhogweed6 libp11-kit0 libidn2-0 libunistring2 libtasn1-6 libpcre2-8-0 libffi8 && \
    apt-get clean && \
    (echo options nbd nbds_max=128 > /etc/modprobe.d/nbd.conf)

COPY --from=build /app/vitastor-csi /bin/

RUN (echo deb http://vitastor.io/debian bookworm main > /etc/apt/sources.list.d/vitastor.list) && \
    ((echo 'Package: *'; echo 'Pin: origin "vitastor.io"'; echo 'Pin-Priority: 1000') > /etc/apt/preferences.d/vitastor.pref) && \
    wget -q -O /etc/apt/trusted.gpg.d/vitastor.gpg https://vitastor.io/debian/pubkey.gpg && \
    apt-get update && \
    apt-get install -y vitastor-client && \
    wget https://vitastor.io/archive/qemu/qemu-bookworm-8.1.2%2Bds-1%2Bvitastor1/qemu-utils_8.1.2%2Bds-1%2Bvitastor1_amd64.deb && \
    wget https://vitastor.io/archive/qemu/qemu-bookworm-8.1.2%2Bds-1%2Bvitastor1/qemu-block-extra_8.1.2%2Bds-1%2Bvitastor1_amd64.deb && \
    dpkg -x qemu-utils*.deb tmp1 && \
    dpkg -x qemu-block-extra*.deb tmp1 && \
    cp -a tmp1/usr/bin/qemu-storage-daemon /usr/bin/ && \
    mkdir -p /usr/lib/x86_64-linux-gnu/qemu && \
    cp -a tmp1/usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/ && \
    rm -rf tmp1 *.deb && \
    apt-get clean

ENTRYPOINT ["/bin/vitastor-csi"]
