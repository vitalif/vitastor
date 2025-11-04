#!/bin/bash

docker build --build-arg DISTRO=debian --build-arg REL=trixie -t vitastor-buildenv:trixie -f vitastor-buildenv.Dockerfile .
docker run -it --rm -e REL=trixie -v `dirname $0`/../:/root/vitastor vitastor-buildenv:trixie /root/vitastor/debian/vitastor-build.sh
