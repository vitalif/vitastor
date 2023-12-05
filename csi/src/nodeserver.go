// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"
    "errors"
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "syscall"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "k8s.io/utils/mount"
    utilexec "k8s.io/utils/exec"

    "github.com/container-storage-interface/spec/lib/go/csi"
    "github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
    "k8s.io/klog"
)

// NodeServer struct of Vitastor CSI driver with supported methods of CSI node server spec.
type NodeServer struct
{
    *Driver
    useVduse bool
    stateDir string
    mounter mount.Interface
}

type DeviceState struct
{
    ConfigPath string `json:"configPath"`
    VdpaId     string `json:"vdpaId"`
    Image      string `json:"image"`
    Blockdev   string `json:"blockdev"`
    Readonly   bool   `json:"readonly"`
    PidFile    string `json:"pidFile"`
}

// NewNodeServer create new instance node
func NewNodeServer(driver *Driver) *NodeServer
{
    stateDir := os.Getenv("STATE_DIR")
    if (stateDir == "")
    {
        stateDir = "/run/vitastor-csi"
    }
    if (stateDir[len(stateDir)-1] != '/')
    {
        stateDir += "/"
    }
    ns := &NodeServer{
        Driver: driver,
        useVduse: checkVduseSupport(),
        stateDir: stateDir,
        mounter: mount.New(""),
    }
    if (ns.useVduse)
    {
        ns.restoreVduseDaemons()
    }
    return ns
}

func checkVduseSupport() bool
{
    // Check VDUSE support (vdpa, vduse, virtio-vdpa kernel modules)
    vduse := true
    for _, mod := range []string{"vdpa", "vduse", "virtio-vdpa"}
    {
        _, err := os.Stat("/sys/module/"+mod)
        if (err != nil)
        {
            if (!errors.Is(err, os.ErrNotExist))
            {
                klog.Errorf("failed to check /sys/module/%s: %v", mod, err)
            }
            c := exec.Command("/sbin/modprobe", mod)
            c.Stdout = os.Stderr
            c.Stderr = os.Stderr
            err := c.Run()
            if (err != nil)
            {
                klog.Errorf("/sbin/modprobe %s failed: %v", mod, err)
                vduse = false
                break
            }
        }
    }
    // Check that vdpa tool functions
    if (vduse)
    {
        c := exec.Command("/sbin/vdpa", "-j", "dev")
        c.Stderr = os.Stderr
        err := c.Run()
        if (err != nil)
        {
            klog.Errorf("/sbin/vdpa -j dev failed: %v", err)
            vduse = false
        }
    }
    if (!vduse)
    {
        klog.Errorf(
            "Your host apparently has no VDUSE support. VDUSE support disabled, NBD will be used to map devices."+
            " For VDUSE you need at least Linux 5.15 and the following kernel modules: vdpa, virtio-vdpa, vduse.",
        )
    }
    return vduse
}

// NodeStageVolume mounts the volume to a staging path on the node.
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error)
{
    return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unstages the volume from the staging path
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error)
{
    return &csi.NodeUnstageVolumeResponse{}, nil
}

func Contains(list []string, s string) bool
{
    for i := 0; i < len(list); i++
    {
        if (list[i] == s)
        {
            return true
        }
    }
    return false
}

func (ns *NodeServer) mapNbd(volName string, ctxVars map[string]string, readonly bool) (string, error)
{
    // Map NBD device
    // FIXME: Check if already mapped
    args := []string{
        "map", "--image", volName,
    }
    if (ctxVars["configPath"] != "")
    {
        args = append(args, "--config_path", ctxVars["configPath"])
    }
    if (readonly)
    {
        args = append(args, "--readonly", "1")
    }
    stdout, stderr, err := system("/usr/bin/vitastor-nbd", args...)
    dev := strings.TrimSpace(string(stdout))
    if (dev == "")
    {
        return "", fmt.Errorf("vitastor-nbd did not return the name of NBD device. output: %s", stderr)
    }
    return dev, err
}

func (ns *NodeServer) unmapNbd(devicePath string)
{
    // unmap NBD device
    unmapOut, unmapErr := exec.Command("/usr/bin/vitastor-nbd", "unmap", devicePath).CombinedOutput()
    if (unmapErr != nil)
    {
        klog.Errorf("failed to unmap NBD device %s: %s, error: %v", devicePath, unmapOut, unmapErr)
    }
}

func findByPidFile(pidFile string) (*os.Process, error)
{
    klog.Infof("killing process with PID from file %s", pidFile)
    pidBuf, err := os.ReadFile(pidFile)
    if (err != nil)
    {
        return nil, err
    }
    pid, err := strconv.ParseInt(strings.TrimSpace(string(pidBuf)), 0, 64)
    if (err != nil)
    {
        return nil, err
    }
    proc, err := os.FindProcess(int(pid))
    if (err != nil)
    {
        return nil, err
    }
    return proc, nil
}

func killByPidFile(pidFile string) error
{
    proc, err := findByPidFile(pidFile)
    if (err != nil)
    {
        return err
    }
    return proc.Signal(syscall.SIGTERM)
}

func startStorageDaemon(vdpaId, volName, pidFile, configPath string, readonly bool) error
{
    // Start qemu-storage-daemon
    blockSpec := map[string]interface{}{
        "node-name": "disk1",
        "driver": "vitastor",
        "image": volName,
        "cache": map[string]bool{
            "direct": true,
            "no-flush": false,
        },
        "discard": "unmap",
    }
    if (configPath != "")
    {
        blockSpec["config-path"] = configPath
    }
    blockSpecJson, _ := json.Marshal(blockSpec)
    writable := "true"
    if (readonly)
    {
        writable = "false"
    }
    _, _, err := system(
        "/usr/bin/qemu-storage-daemon", "--daemonize", "--pidfile", pidFile, "--blockdev", string(blockSpecJson),
        "--export", "vduse-blk,id="+vdpaId+",node-name=disk1,name="+vdpaId+",num-queues=16,queue-size=128,writable="+writable,
    )
    return err
}

func (ns *NodeServer) mapVduse(volName string, ctxVars map[string]string, readonly bool) (string, string, error)
{
    // Generate state file
    stateFd, err := os.CreateTemp(ns.stateDir, "vitastor-vduse-*.json")
    if (err != nil)
    {
        return "", "", err
    }
    stateFile := stateFd.Name()
    stateFd.Close()
    vdpaId := filepath.Base(stateFile)
    vdpaId = vdpaId[0:len(vdpaId)-5] // remove ".json"
    pidFile := ns.stateDir + vdpaId + ".pid"
    // Map VDUSE device via qemu-storage-daemon
    err = startStorageDaemon(vdpaId, volName, pidFile, ctxVars["configPath"], readonly)
    if (err == nil)
    {
        // Add device to VDPA bus
        _, _, err = system("/sbin/vdpa", "-j", "dev", "add", "name", vdpaId, "mgmtdev", "vduse")
        if (err == nil)
        {
            // Find block device name
            matches, err := filepath.Glob("/sys/bus/vdpa/devices/"+vdpaId+"/virtio*/block/*")
            if (err == nil && len(matches) == 0)
            {
                err = errors.New("/sys/bus/vdpa/devices/"+vdpaId+"/virtio*/block/* is not found")
            }
            if (err == nil)
            {
                blockdev := "/dev/"+filepath.Base(matches[0])
                _, err = os.Stat(blockdev)
                if (err == nil)
                {
                    // Generate state file
                    stateJSON, _ := json.Marshal(&DeviceState{
                        ConfigPath: ctxVars["configPath"],
                        VdpaId:     vdpaId,
                        Image:      volName,
                        Blockdev:   blockdev,
                        Readonly:   readonly,
                        PidFile:    pidFile,
                    })
                    err = os.WriteFile(stateFile, stateJSON, 0600)
                    if (err == nil)
                    {
                        return blockdev, vdpaId, nil
                    }
                }
            }
        }
        killErr := killByPidFile(pidFile)
        if (killErr != nil)
        {
            klog.Errorf("Failed to kill started qemu-storage-daemon: %v", killErr)
        }
        os.Remove(stateFile)
        os.Remove(pidFile)
    }
    return "", "", err
}

func (ns *NodeServer) unmapVduse(devicePath string)
{
    if (len(devicePath) < 6 || devicePath[0:6] != "/dev/v")
    {
        klog.Errorf("%s does not start with /dev/v", devicePath)
        return
    }
    vduseDev, err := os.Readlink("/sys/block/"+devicePath[5:])
    if (err != nil)
    {
        klog.Errorf("%s is not a symbolic link to VDUSE device (../devices/virtual/vduse/xxx): %v", devicePath, err)
        return
    }
    vdpaId := ""
    p := strings.Index(vduseDev, "/vduse/")
    if (p >= 0)
    {
        vduseDev = vduseDev[p+7:]
        p = strings.Index(vduseDev, "/")
        if (p >= 0)
        {
            vdpaId = vduseDev[0:p]
        }
    }
    if (vdpaId == "")
    {
        klog.Errorf("%s is not a symbolic link to VDUSE device (../devices/virtual/vduse/xxx), but is %v", devicePath, vduseDev)
        return
    }
    ns.unmapVduseById(vdpaId)
}

func (ns *NodeServer) unmapVduseById(vdpaId string)
{
    _, err := os.Stat("/sys/bus/vdpa/devices/"+vdpaId)
    if (err != nil)
    {
        klog.Errorf("failed to stat /sys/bus/vdpa/devices/"+vdpaId+": %v", err)
    }
    else
    {
        _, _, _ = system("/sbin/vdpa", "-j", "dev", "del", vdpaId)
    }
    stateFile := ns.stateDir + vdpaId + ".json"
    os.Remove(stateFile)
    pidFile := ns.stateDir + vdpaId + ".pid"
    _, err = os.Stat(pidFile)
    if (os.IsNotExist(err))
    {
        // ok, already killed
    }
    else if (err != nil)
    {
        klog.Errorf("Failed to stat %v: %v", pidFile, err)
        return
    }
    else
    {
        err = killByPidFile(pidFile)
        if (err != nil)
        {
            klog.Errorf("Failed to kill started qemu-storage-daemon: %v", err)
        }
        os.Remove(pidFile)
    }
}

func (ns *NodeServer) restoreVduseDaemons()
{
    pattern := ns.stateDir+"vitastor-vduse-*.json"
    matches, err := filepath.Glob(pattern)
    if (err != nil)
    {
        klog.Errorf("failed to list %s: %v", pattern, err)
    }
    if (len(matches) == 0)
    {
        return
    }
    devList := make(map[string]interface{})
    // example output: {"dev":{"test1":{"type":"block","mgmtdev":"vduse","vendor_id":0,"max_vqs":16,"max_vq_size":128}}}
    devListJSON, _, err := system("/sbin/vdpa", "-j", "dev", "list")
    if (err != nil)
    {
        return
    }
    err = json.Unmarshal(devListJSON, &devList)
    devs, ok := devList["dev"].(map[string]interface{})
    if (err != nil || !ok)
    {
        klog.Errorf("/sbin/vdpa -j dev list returned bad JSON (error %v): %v", err, string(devListJSON))
        return
    }
    for _, stateFile := range matches
    {
        vdpaId := filepath.Base(stateFile)
        vdpaId = vdpaId[0:len(vdpaId)-5]
        // Check if VDPA device is still added to the bus
        if (devs[vdpaId] != nil)
        {
            // Check if the storage daemon is still active
            pidFile := ns.stateDir + vdpaId + ".pid"
            exists := false
            proc, err := findByPidFile(pidFile)
            if (err == nil)
            {
                exists = proc.Signal(syscall.Signal(0)) == nil
            }
            if (!exists)
            {
                // Restart daemon
                stateJSON, err := os.ReadFile(stateFile)
                if (err != nil)
                {
                    klog.Warningf("error reading state file %v: %v", stateFile, err)
                }
                else
                {
                    var state DeviceState
                    err := json.Unmarshal(stateJSON, &state)
                    if (err != nil)
                    {
                        klog.Warningf("state file %v contains invalid JSON (error %v): %v", stateFile, err, string(stateJSON))
                    }
                    else
                    {
                        klog.Warningf("restarting storage daemon for volume %v (VDPA ID %v)", state.Image, vdpaId)
                        _ = startStorageDaemon(vdpaId, state.Image, pidFile, state.ConfigPath, state.Readonly)
                    }
                }
            }
        }
        else
        {
            // Unused, clean it up
            ns.unmapVduseById(vdpaId)
        }
    }
}

// NodePublishVolume mounts the volume mounted to the staging path to the target path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error)
{
    klog.Infof("received node publish volume request %+v", protosanitizer.StripSecrets(req))

    targetPath := req.GetTargetPath()
    isBlock := req.GetVolumeCapability().GetBlock() != nil

    // Check that it's not already mounted
    _, err := mount.IsNotMountPoint(ns.mounter, targetPath)
    if (err != nil)
    {
        if (os.IsNotExist(err))
        {
            if (isBlock)
            {
                pathFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR, 0o600)
                if (err != nil)
                {
                    klog.Errorf("failed to create block device mount target %s with error: %v", targetPath, err)
                    return nil, err
                }
                err = pathFile.Close()
                if (err != nil)
                {
                    klog.Errorf("failed to close %s with error: %v", targetPath, err)
                    return nil, err
                }
            }
            else
            {
                err := os.MkdirAll(targetPath, 0777)
                if (err != nil)
                {
                    klog.Errorf("failed to create fs mount target %s with error: %v", targetPath, err)
                    return nil, err
                }
            }
        }
        else
        {
            return nil, err
        }
    }

    ctxVars := make(map[string]string)
    err = json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    _, err = GetConnectionParams(ctxVars)
    if (err != nil)
    {
        return nil, err
    }

    var devicePath, vdpaId string
    if (!ns.useVduse)
    {
        devicePath, err = ns.mapNbd(volName, ctxVars, req.GetReadonly())
    }
    else
    {
        devicePath, vdpaId, err = ns.mapVduse(volName, ctxVars, req.GetReadonly())
    }
    if (err != nil)
    {
        return nil, err
    }

    diskMounter := &mount.SafeFormatAndMount{Interface: ns.mounter, Exec: utilexec.New()}
    if (isBlock)
    {
        err = diskMounter.Mount(devicePath, targetPath, "", []string{"bind"})
    }
    else
    {
        // Check existing format
        existingFormat, err := diskMounter.GetDiskFormat(devicePath)
        if (err != nil)
        {
            klog.Errorf("failed to get disk format for path %s, error: %v", err)
            goto unmap
        }

        // Format the device (ext4 or xfs)
        fsType := req.GetVolumeCapability().GetMount().GetFsType()
        opt := req.GetVolumeCapability().GetMount().GetMountFlags()
        opt = append(opt, "_netdev")
        if ((req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
            req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY) &&
            !Contains(opt, "ro"))
        {
            opt = append(opt, "ro")
        }
        if (fsType == "xfs")
        {
            opt = append(opt, "nouuid")
        }
        readOnly := Contains(opt, "ro")
        if (existingFormat == "" && !readOnly)
        {
            var cmdOut []byte
            switch fsType
            {
                case "ext4":
                    args := []string{"-m0", "-Enodiscard,lazy_itable_init=1,lazy_journal_init=1", devicePath}
                    cmdOut, err = diskMounter.Exec.Command("mkfs.ext4", args...).CombinedOutput()
                case "xfs":
                    cmdOut, err = diskMounter.Exec.Command("mkfs.xfs", "-K", devicePath).CombinedOutput()
            }
            if (err != nil)
            {
                klog.Errorf("failed to run mkfs error: %v, output: %v", err, string(cmdOut))
                goto unmap
            }
        }

        err = diskMounter.FormatAndMount(devicePath, targetPath, fsType, opt)

        // Try to run online resize on mount.
        // FIXME: Implement online resize. It requires online resize support in vitastor-nbd.
        if (err == nil && existingFormat != "" && !readOnly)
        {
            var cmdOut []byte
            switch (fsType)
            {
                case "ext4":
                    cmdOut, err = diskMounter.Exec.Command("resize2fs", devicePath).CombinedOutput()
                case "xfs":
                    cmdOut, err = diskMounter.Exec.Command("xfs_growfs", devicePath).CombinedOutput()
            }
            if (err != nil)
            {
                klog.Errorf("failed to run resizefs error: %v, output: %v", err, string(cmdOut))
                goto unmap
            }
        }
    }
    if (err != nil)
    {
        klog.Errorf(
            "failed to mount device path (%s) to path (%s) for volume (%s) error: %s",
            devicePath, targetPath, volName, err,
        )
        goto unmap
    }
    return &csi.NodePublishVolumeResponse{}, nil

unmap:
    if (!ns.useVduse || len(devicePath) >= 8 && devicePath[0:8] == "/dev/nbd")
    {
        ns.unmapNbd(devicePath)
    }
    else
    {
        ns.unmapVduseById(vdpaId)
    }
    return nil, err
}

// NodeUnpublishVolume unmounts the volume from the target path
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error)
{
    klog.Infof("received node unpublish volume request %+v", protosanitizer.StripSecrets(req))
    targetPath := req.GetTargetPath()
    devicePath, refCount, err := mount.GetDeviceNameFromMount(ns.mounter, targetPath)
    if (err != nil)
    {
        if (os.IsNotExist(err))
        {
            return nil, status.Error(codes.NotFound, "Target path not found")
        }
        return nil, err
    }
    if (devicePath == "")
    {
        // volume not mounted
        klog.Warningf("%s is not a mountpoint, deleting", targetPath)
        os.Remove(targetPath)
        return &csi.NodeUnpublishVolumeResponse{}, nil
    }
    // unmount
    err = mount.CleanupMountPoint(targetPath, ns.mounter, false)
    if (err != nil)
    {
        return nil, err
    }
    // unmap NBD device
    if (refCount == 1)
    {
        if (!ns.useVduse)
        {
            ns.unmapNbd(devicePath)
        }
        else
        {
            ns.unmapVduse(devicePath)
        }
    }
    return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats returns volume capacity statistics available for the volume
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// NodeExpandVolume expanding the file system on the node
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// NodeGetCapabilities returns the supported capabilities of the node server
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error)
{
    return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodeGetInfo returns NodeGetInfoResponse for CO.
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error)
{
    klog.Infof("received node get info request %+v", protosanitizer.StripSecrets(req))
    return &csi.NodeGetInfoResponse{
        NodeId: ns.NodeID,
    }, nil
}
