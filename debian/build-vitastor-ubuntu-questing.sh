#!/bin/bash
# 25.10 Questing quokka

docker build --build-arg DISTRO=ubuntu --build-arg REL=questing -t vitastor-buildenv:questing -f vitastor-buildenv.Dockerfile .
docker run -it --rm -e REL=questing -v `dirname $0`/../:/root/vitastor vitastor-buildenv:questing /root/vitastor/debian/vitastor-build.sh
