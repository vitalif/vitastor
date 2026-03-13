#!/bin/bash

set -e

cp -urv /etc/systemd/system/vitastor* /host-etc/systemd/system/
cp -urv /etc/udev/rules.d /host-etc/udev/
cp -urnv /etc/vitastor /host-etc/
cp -urnv /opt/scripts/* /host-bin/
