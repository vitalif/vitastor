#!/bin/bash

set -e

cp -urv /etc/default /host-etc/
cp -urv /etc/systemd /host-etc/
cp -urv /etc/udev /host-etc/
cp -urnv /etc/vitastor /host-etc/
cp -urnv /opt/scripts/* /host-bin/
