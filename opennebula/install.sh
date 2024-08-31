#!/bin/bash

set -e

reapply_patch() {
	if ! patch -f --dry-run -l -R $1 < $2 >/dev/null; then
		already_applied=0
		if ! patch -l -f $1 < $2; then
			applied_ok=0
			echo "ERROR: Failed to patch file $1, please apply the patch $2 manually"
		fi
	fi
}
echo "Reapplying Vitastor patches to OpenNebula's oned.conf, vmm_execrc and downloader.sh"
already_applied=1
applied_ok=1
reapply_patch /var/lib/one/remotes/datastore/downloader.sh /var/lib/one/remotes/datastore/vitastor/downloader-vitastor.sh.diff
reapply_patch /etc/one/oned.conf /var/lib/one/remotes/datastore/vitastor/oned.conf.diff
reapply_patch /etc/one/vmm_exec/vmm_execrc /var/lib/one/remotes/datastore/vitastor/vmm_execrc.diff
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
