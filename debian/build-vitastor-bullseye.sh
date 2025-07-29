#!/bin/bash

docker build --build-arg DISTRO=debian --build-arg REL=bullseye -t vitastor-buildenv:bullseye -f vitastor-buildenv.Dockerfile .
docker run -i --rm -e REL=bullseye -v `dirname $0`/../:/root/vitastor vitastor-buildenv:bullseye /root/vitastor/debian/vitastor-build.sh
