#!/bin/bash

set -e

reapply_patch() {
	if ! [[ -e $1 ]]; then
		echo "$1 does not exist, OpenNebula is not installed"
	elif ! patch -f --dry-run -F 0 -R $1 < $2 >/dev/null; then
		already_applied=0
		if ! patch --no-backup-if-mismatch -r - -F 0 -f $1 < $2; then
			applied_ok=0
			echo "ERROR: Failed to patch file $1, please apply the patch $2 manually"
		fi
	fi
}
echo "Reapplying Vitastor patches to OpenNebula's oned.conf, vmm_execrc and downloader.sh"
already_applied=1
applied_ok=1
reapply_patch /var/lib/one/remotes/datastore/downloader.sh /var/lib/one/remotes/datastore/vitastor/downloader-vitastor.sh.diff
reapply_patch /etc/one/vmm_exec/vmm_execrc /var/lib/one/remotes/datastore/vitastor/vmm_execrc.diff
if [[ -e /etc/one/oned.conf ]]; then
	if ! /var/lib/one/remotes/datastore/vitastor/patch-oned-conf.py /etc/one/oned.conf; then
		applied_ok=0
		already_applied=0
	fi
fi
if [[ "$already_applied" = 1 ]]; then
	echo "OK: Vitastor OpenNebula patches are already applied"
elif [[ "$applied_ok" = 1 ]]; then
	echo "OK: Vitastor OpenNebula patches successfully applied"
fi
if [[ -f /etc/apparmor.d/local/abstractions/libvirt-qemu ]]; then
	if ! grep -q /etc/vitastor/vitastor.conf /etc/apparmor.d/local/abstractions/libvirt-qemu; then
		echo '  "/etc/vitastor/vitastor.conf" r,' >> /etc/apparmor.d/local/abstractions/libvirt-qemu
	fi
fi
