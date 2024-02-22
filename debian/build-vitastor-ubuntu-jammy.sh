#!/bin/bash

cat < vitastor.Dockerfile > ../Dockerfile
cd ..
mkdir -p packages
sudo podman build --build-arg DISTRO=ubuntu --build-arg REL=jammy -v `pwd`/packages:/root/packages -f Dockerfile .
rm Dockerfile
