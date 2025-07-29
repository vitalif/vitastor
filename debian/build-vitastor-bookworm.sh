#!/bin/bash

docker build --build-arg DISTRO=debian --build-arg REL=bookworm -t vitastor-buildenv:bookworm -f vitastor-buildenv.Dockerfile .
docker run --rm -e REL=bookworm -v `dirname $0`/../:/root/vitastor vitastor-buildenv:bookworm /root/vitastor/debian/vitastor-build.sh
