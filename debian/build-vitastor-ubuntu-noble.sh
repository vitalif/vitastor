#!/bin/bash
# 24.04 Noble Numbat

docker build --build-arg DISTRO=ubuntu --build-arg REL=noble -t vitastor-buildenv:noble -f vitastor-buildenv.Dockerfile .
docker run --rm -e REL=noble -v `dirname $0`/../:/root/vitastor vitastor-buildenv:noble /root/vitastor/debian/vitastor-build.sh
