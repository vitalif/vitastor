#!/bin/bash
# 26.04 Resolute Raccoon

docker build --build-arg DISTRO=ubuntu --build-arg REL=resolute -t vitastor-buildenv:resolute -f vitastor-buildenv.Dockerfile .
docker run -it --rm -e REL=resolute -v `dirname $0`/../:/root/vitastor vitastor-buildenv:resolute /root/vitastor/debian/vitastor-build.sh
