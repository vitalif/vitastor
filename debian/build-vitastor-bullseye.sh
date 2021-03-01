#!/bin/bash

sed 's/$REL/bullseye/g' < vitastor.Dockerfile > ../Dockerfile
cd ..
mkdir -p packages
sudo podman build -v `pwd`/packages:/root/packages -f Dockerfile .
rm Dockerfile
