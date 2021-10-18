# Compile stage
FROM golang:buster AS build

ADD go.sum go.mod /app/
RUN cd /app; CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go mod download -x
ADD . /app
RUN perl -i -e '$/ = undef; while(<>) { s/\n\s*(\{\s*\n)/$1\n/g; s/\}(\s*\n\s*)else\b/$1} else/g; print; }' `find /app -name '*.go'`
RUN cd /app; CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o vitastor-csi

# Final stage
FROM debian:buster

LABEL maintainers="Vitaliy Filippov <vitalif@yourcmc.ru>"
LABEL description="Vitastor CSI Driver"

ENV NODE_ID=""
ENV CSI_ENDPOINT=""

RUN apt-get update && \
    apt-get install -y wget && \
    wget -q -O /etc/apt/trusted.gpg.d/vitastor.gpg https://vitastor.io/debian/pubkey.gpg && \
    (echo deb http://vitastor.io/debian buster main > /etc/apt/sources.list.d/vitastor.list) && \
    (echo deb http://deb.debian.org/debian buster-backports main > /etc/apt/sources.list.d/backports.list) && \
    (echo "APT::Install-Recommends false;" > /etc/apt/apt.conf) && \
    apt-get update && \
    apt-get install -y e2fsprogs xfsprogs vitastor kmod && \
    apt-get clean && \
    (echo options nbd nbds_max=128 > /etc/modprobe.d/nbd.conf)

COPY --from=build /app/vitastor-csi /bin/

ENTRYPOINT ["/bin/vitastor-csi"]
