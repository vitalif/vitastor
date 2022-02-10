#!/bin/bash

if test $(id -u) != 0; then
	SUDO=sudo
fi

if [ y`uname`y = yLinuxy ]; then
	source /etc/os-release
	case $ID in
	debian)
		$SUDO apt-get install -y lp-solve linux-image-amd64 nodejs sudo gnupg devscripts gcc debhelper cmake glib-2.0 libibverbs-dev g++ pkg-config liburing-dev libtcmalloc-minimal4 libgoogle-perftools-dev libjerasure-dev jq npm gdisk
		;;
	esac
fi
