# Build Docker image with Vitastor packages

FROM debian:bullseye

ADD vitastor.list /etc/apt/sources.list.d
ADD vitastor.gpg /etc/apt/trusted.gpg.d
ADD vitastor.pref /etc/apt/preferences.d
ADD apt.conf /etc/apt/
RUN apt-get update && apt-get -y install vitastor qemu-system-x86 qemu-system-common && apt-get clean
