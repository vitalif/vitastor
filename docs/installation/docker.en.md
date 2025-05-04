[Documentation](../../README.md#documentation) → Installation → Dockerized Installation

-----

[Читать на русском](docker.ru.md)

# Dockerized Installation

Vitastor may be installed in Docker/Podman. In such setups etcd, monitors and OSD
all run in containers, but everything else looks as close as possible to a usual
setup with packages:
- host network is used
- auto-start is implemented through udev and systemd
- logs are written to journald (not docker json log files)
- command-line wrapper scripts are installed to the host system to call vitastor-disk,
  vitastor-cli and others through the container

Such installations may be useful when it's impossible or inconvenient to install
Vitastor from packages, for example, in exotic Linux distributions.

If you don't want just a simple containerized installation, you can also take a look
at Vitastor Kubernetes operator: https://github.com/Antilles7227/vitastor-operator

## Installing Containers

The instruction is very simple.

1. Download a Docker image of the desired version: \
   `docker pull vitastor:v2.1.0`
2. Install scripts to the host system: \
   `docker run --rm -it -v /etc:/host-etc -v /usr/bin:/host-bin vitastor:v2.1.0 install.sh`
3. Reload udev rules: \
   `udevadm control --reload-rules`

And you can return to [Quick Start](../intro/quickstart.en.md).

## Upgrading Containers

First make sure to check the topic [Upgrading Vitastor](../usage/admin.en.md#upgrading-vitastor)
to figure out if you need any additional steps.

Then, to upgrade a containerized installation, you just need to change the `VITASTOR_VERSION`
option in `/etc/vitastor/docker.conf` and restart all Vitastor services:

`systemctl restart vitastor.target`

## QEMU

Vitastor Docker image also contains QEMU, qemu-img and qemu-storage-daemon built with Vitastor support.

However, running QEMU in Docker is harder to setup and it depends on the used virtualization UI
(OpenNebula, Proxmox and so on). Some of them also required patched Libvirt.

That's why containerized installation of Vitastor doesn't contain a ready-made QEMU setup and it's
recommended to install QEMU from packages or build it manually.

## fio

Vitastor Docker image also contains fio and installs a wrapper called `vitastor-fio` to use it from
the host system.
