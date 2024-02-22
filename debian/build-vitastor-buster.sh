#!/bin/bash

cat < vitastor.Dockerfile > ../Dockerfile
cd ..
mkdir -p packages
sudo podman build --build-arg DISTRO=debian --build-arg REL=buster -v `pwd`/packages:/root/packages -f Dockerfile .
rm Dockerfile
