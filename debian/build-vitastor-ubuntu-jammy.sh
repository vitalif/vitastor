#!/bin/bash
# Ubuntu 22.04 Jammy Jellyfish

docker build --build-arg DISTRO=ubuntu --build-arg REL=jammy -t vitastor-buildenv:jammy -f vitastor-buildenv.Dockerfile .
docker run -i --rm -e REL=jammy -v `dirname $0`/../:/root/vitastor vitastor-buildenv:jammy /root/vitastor/debian/vitastor-build.sh
