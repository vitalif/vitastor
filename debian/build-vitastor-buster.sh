#!/bin/bash

docker build --build-arg DISTRO=debian --build-arg REL=buster -t vitastor-buildenv:buster -f vitastor-buildenv.Dockerfile .
docker run -it --rm -e REL=buster -v `dirname $0`/../:/root/vitastor vitastor-buildenv:buster /root/vitastor/debian/vitastor-build.sh
