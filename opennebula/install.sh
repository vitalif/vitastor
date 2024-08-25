#!/bin/bash

set -e

reapply_patch() {
	if ! patch -f --dry-run -l -R $1 < $2 >/dev/null; then
		if ! patch -l -f $1 < $2; then
			echo "ERROR: Failed to patch file $1, please apply the patch $2 manually"
		fi
	fi
}
echo "Reapplying Vitastor patches to OpenNebula's oned.conf, vmm_execrc and downloader.sh"
reapply_patch /var/lib/one/remotes/datastore/downloader.sh /var/lib/one/remotes/datastore/vitastor/downloader-vitastor.sh.diff
reapply_patch /etc/one/oned.conf /var/lib/one/remotes/datastore/vitastor/oned.conf.diff
reapply_patch /etc/one/vmm_exec/vmm_execrc /var/lib/one/remotes/datastore/vitastor/vmm_execrc.diff
if [ -f /etc/apparmor.d/local/abstractions/libvirt-qemu ]; then
	if ! grep -q /etc/vitastor/vitastor.conf /etc/apparmor.d/local/abstractions/libvirt-qemu; then
		echo '  "/etc/vitastor/vitastor.conf" r,' >> /etc/apparmor.d/local/abstractions/libvirt-qemu
	fi
fi
