exit

git clone https://git.yourcmc.ru/vitalif/pve-qemu .

# bookworm

docker run -it -v `pwd`/pve-qemu:/root/pve-qemu --name pve-qemu-bullseye debian:bullseye bash

perl -i -pe 's/Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources
echo 'deb [arch=amd64] http://download.proxmox.com/debian/pve bookworm pve-no-subscription' >> /etc/apt/sources.list
echo 'deb https://vitastor.io/debian bookworm main' >> /etc/apt/sources.list
echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf
echo 'ru_RU UTF-8' >> /etc/locale.gen
echo 'en_US UTF-8' >> /etc/locale.gen
apt-get update
apt-get install wget ca-certificates
wget https://enterprise.proxmox.com/debian/proxmox-release-bookworm.gpg -O /etc/apt/trusted.gpg.d/proxmox-release-bookworm.gpg
wget https://vitastor.io/debian/pubkey.gpg -O /etc/apt/trusted.gpg.d/vitastor.gpg
apt-get update
apt-get install git devscripts equivs wget mc libjemalloc-dev vitastor-client-dev lintian locales
mk-build-deps --install ./control

# bullseye

docker run -it -v `pwd`/pve-qemu:/root/pve-qemu --name pve-qemu-bullseye debian:bullseye bash

grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb /deb-src /' >> /etc/apt/sources.list
echo 'deb [arch=amd64] http://download.proxmox.com/debian/pve bullseye pve-no-subscription' >> /etc/apt/sources.list
echo 'deb https://vitastor.io/debian bullseye main' >> /etc/apt/sources.list
echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf
echo 'ru_RU UTF-8' >> /etc/locale.gen
echo 'en_US UTF-8' >> /etc/locale.gen
apt-get update
apt-get install wget
wget https://enterprise.proxmox.com/debian/proxmox-release-bullseye.gpg -O /etc/apt/trusted.gpg.d/proxmox-release-bullseye.gpg
wget https://vitastor.io/debian/pubkey.gpg -O /etc/apt/trusted.gpg.d/vitastor.gpg
apt-get update
apt-get install git devscripts equivs wget mc libjemalloc-dev vitastor-client-dev lintian locales
mk-build-deps --install ./control

# buster

docker run -it -v `pwd`/pve-qemu:/root/pve-qemu --name pve-qemu-buster debian:buster bash

grep '^deb ' /etc/apt/sources.list | perl -pe 's/^deb /deb-src /' >> /etc/apt/sources.list
echo 'deb [arch=amd64] http://download.proxmox.com/debian/pve buster pve-no-subscription' >> /etc/apt/sources.list
echo 'deb https://vitastor.io/debian buster main' >> /etc/apt/sources.list
echo 'deb http://deb.debian.org/debian buster-backports main' >> /etc/apt/sources.list
echo 'APT::Install-Recommends false;' >> /etc/apt/apt.conf
echo 'ru_RU UTF-8' >> /etc/locale.gen
echo 'en_US UTF-8' >> /etc/locale.gen
apt-get update
apt-get install wget ca-certificates
wget http://download.proxmox.com/debian/proxmox-ve-release-6.x.gpg -O /etc/apt/trusted.gpg.d/proxmox-ve-release-6.x.gpg
wget https://vitastor.io/debian/pubkey.gpg -O /etc/apt/trusted.gpg.d/vitastor.gpg
apt-get update
apt-get install git devscripts equivs wget mc libjemalloc-dev vitastor-client-dev lintian locales
mk-build-deps --install ./control
