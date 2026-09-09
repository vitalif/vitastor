// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "reflect"
    "testing"

    "k8s.io/utils/mount"
)

func TestCollectActiveNFS(t *testing.T)
{
    mounts := []mount.MountInfo{
        {
            Root:         "/",
            Source:       "127.0.0.1:/",
            MountPoint:   "/var/lib/kubelet/plugins/csi.vitastor.io/nfs/hash",
            FsType:       "nfs",
            MountOptions: []string{"rw", "relatime"},
            SuperOptions: []string{"rw", "vers=3", "port=45059"},
        },
        {
            // This is how NFS subdirectory bind mounts appear on Linux 6.19:
            // Root is still / and the subdirectory is only visible in Source.
            Root:         "/",
            Source:       "127.0.0.1:/pvc-one",
            MountPoint:   "/var/lib/kubelet/pods/pod-one/volumes/kubernetes.io~csi/pvc-one/mount",
            FsType:       "nfs",
            MountOptions: []string{"rw", "relatime"},
            SuperOptions: []string{"rw", "vers=3", "port=45059"},
        },
        {
            Root:         "/",
            Source:       "127.0.0.1:/pvc-two",
            MountPoint:   "/var/lib/kubelet/pods/pod-two/volumes/kubernetes.io~csi/pvc-two/mount",
            FsType:       "nfs",
            MountOptions: []string{"rw", "relatime"},
            SuperOptions: []string{"rw", "vers=3", "port=45059"},
        },
        {
            // Older kernels expose the bind-mounted subdirectory in Root.
            Root:         "/pvc-three",
            Source:       "127.0.0.1:/",
            MountPoint:   "/var/lib/kubelet/pods/pod-three/volumes/kubernetes.io~csi/pvc-three/mount",
            FsType:       "nfs",
            MountOptions: []string{"rw", "relatime"},
            SuperOptions: []string{"rw", "vers=3", "port=45059"},
        },
        {
            Root:         "/",
            Source:       "server:/other",
            MountPoint:   "/mnt/other",
            FsType:       "nfs",
            MountOptions: []string{"rw"},
            SuperOptions: []string{"rw", "port=2049"},
        },
    }

    got := collectActiveNFS(mounts)
    want := map[int][]string{
        45059: {
            "/var/lib/kubelet/pods/pod-one/volumes/kubernetes.io~csi/pvc-one/mount",
            "/var/lib/kubelet/pods/pod-two/volumes/kubernetes.io~csi/pvc-two/mount",
            "/var/lib/kubelet/pods/pod-three/volumes/kubernetes.io~csi/pvc-three/mount",
        },
    }
    if (!reflect.DeepEqual(got, want))
    {
        t.Fatalf("unexpected active NFS mounts: got %#v, want %#v", got, want)
    }
}
