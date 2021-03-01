#!/bin/bash

sed 's/$REL/buster/g' < vitastor.Dockerfile > ../Dockerfile
cd ..
mkdir -p packages
sudo podman build -v `pwd`/packages:/root/packages -f Dockerfile .
rm Dockerfile
