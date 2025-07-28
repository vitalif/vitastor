#!/bin/bash
# 24.04 Noble Numbat

cat < vitastor.Dockerfile > ../Dockerfile
cd ..
mkdir -p packages
sudo podman build --build-arg DISTRO=ubuntu --build-arg REL=noble -v `pwd`/packages:/root/packages -f Dockerfile .
rm Dockerfile
