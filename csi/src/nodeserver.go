// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"
    "crypto/sha1"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"

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
    useVduse        bool
    stateDir        string
    nfsStageDir     string
    mounter         mount.Interface
    restartInterval time.Duration
    mu              sync.Mutex
    cond            *sync.Cond
    volumeLocks     map[string]bool
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

type NfsState struct
{
    ConfigPath string `json:"configPath"`
    FsName     string `json:"fsName"`
    Pool       string `json:"pool"`
    Path       string `json:"path"`
    Port       int    `json:"port"`
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
    nfsStageDir := os.Getenv("NFS_STAGE_DIR")
    if (nfsStageDir == "")
    {
        nfsStageDir = "/var/lib/kubelet/plugins/csi.vitastor.io/nfs"
    }
    ns := &NodeServer{
        Driver:      driver,
        useVduse:    checkVduseSupport(),
        stateDir:    stateDir,
        nfsStageDir: nfsStageDir,
        mounter:     mount.New(""),
        volumeLocks: make(map[string]bool),
    }
    ns.cond = sync.NewCond(&ns.mu)
    if (ns.useVduse)
    {
        ns.restoreVduseDaemons()
        dur, err := time.ParseDuration(os.Getenv("RESTART_INTERVAL"))
        if (err != nil)
        {
            dur = 10 * time.Second
        }
        ns.restartInterval = dur
        if (ns.restartInterval != time.Duration(0))
        {
            go ns.restarter()
        }
    }
    return ns
}

func (ns *NodeServer) lockVolume(lockId string)
{
    ns.mu.Lock()
    defer ns.mu.Unlock()
    for (ns.volumeLocks[lockId])
    {
        ns.cond.Wait()
    }
    ns.volumeLocks[lockId] = true
    ns.cond.Broadcast()
}

func (ns *NodeServer) unlockVolume(lockId string)
{
    ns.mu.Lock()
    defer ns.mu.Unlock()
    delete(ns.volumeLocks, lockId)
    ns.cond.Broadcast()
}

func (ns *NodeServer) restarter()
{
    // Restart dead VDUSE daemons at regular intervals
    // Otherwise volume I/O may hang in case of a qemu-storage-daemon crash
    // Moreover, it may lead to a kernel panic of the kernel is configured to
    // panic on hung tasks
    ticker := time.NewTicker(ns.restartInterval)
    defer ticker.Stop()
    for
    {
        <-ticker.C
        ns.restoreVduseDaemons()
    }
}

func (ns *NodeServer) restoreVduseDaemons()
{
    pattern := ns.stateDir+"vitastor-vduse-*.json"
    stateFiles, err := filepath.Glob(pattern)
    if (err != nil)
    {
        klog.Errorf("failed to list %s: %v", pattern, err)
    }
    if (len(stateFiles) == 0)
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
    for _, stateFile := range stateFiles
    {
        ns.checkVduseState(stateFile, devs)
    }
}

func (ns *NodeServer) checkVduseState(stateFile string, devs map[string]interface{})
{
    // Check if VDPA device is still added to the bus
    vdpaId := filepath.Base(stateFile)
    vdpaId = vdpaId[0:len(vdpaId)-5]
    if (devs[vdpaId] == nil)
    {
        // Unused, clean it up
        unmapVduseById(ns.stateDir, vdpaId)
        return
    }

    // Read state file
    stateJSON, err := os.ReadFile(stateFile)
    if (err != nil)
    {
        klog.Warningf("error reading state file %v: %v", stateFile, err)
        return
    }
    var state DeviceState
    err = json.Unmarshal(stateJSON, &state)
    if (err != nil)
    {
        klog.Warningf("state file %v contains invalid JSON (error %v): %v", stateFile, err, string(stateJSON))
        return
    }

    // Lock volume
    ns.lockVolume(state.ConfigPath+":block:"+state.Image)
    defer ns.unlockVolume(state.ConfigPath+":block:"+state.Image)

    // Recheck state file after locking
    _, err = os.ReadFile(stateFile)
    if (err != nil)
    {
        klog.Warningf("state file %v disappeared, skipping volume", stateFile)
        return
    }

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
        klog.Warningf("restarting storage daemon for volume %v (VDPA ID %v)", state.Image, vdpaId)
        err = startStorageDaemon(vdpaId, state.Image, pidFile, state.ConfigPath, state.Readonly)
        if (err != nil)
        {
            klog.Warningf("failed to restart storage daemon for volume %v: %v", state.Image, err)
        }
    }
}

func (ns *NodeServer) restoreNfsDaemons()
{
    pattern := ns.stateDir+"vitastor-nfs-*.json"
    stateFiles, err := filepath.Glob(pattern)
    if (err != nil)
    {
        klog.Errorf("failed to list %s: %v", pattern, err)
    }
    if (len(stateFiles) == 0)
    {
        return
    }
    activeNFS, err := ns.listActiveNFS()
    if (err != nil)
    {
        return
    }
    // Check all state files and try to restore active mounts
    for _, stateFile := range stateFiles
    {
        ns.checkNfsState(stateFile, activeNFS)
    }
}

func (ns *NodeServer) readNfsState(stateFile string, allowNotExists bool) (*NfsState, error)
{
    stateJSON, err := os.ReadFile(stateFile)
    if (err != nil)
    {
        if (allowNotExists && os.IsNotExist(err))
        {
            return nil, nil
        }
        klog.Warningf("error reading state file %v: %v", stateFile, err)
        return nil, err
    }
    var state NfsState
    err = json.Unmarshal(stateJSON, &state)
    if (err != nil)
    {
        klog.Warningf("state file %v contains invalid JSON (error %v): %v", stateFile, err, string(stateJSON))
        return nil, err
    }
    return &state, nil
}

func (ns *NodeServer) checkNfsState(stateFile string, activeNfs map[int][]string)
{
    // Read state file
    state, err := ns.readNfsState(stateFile, false)
    if (err != nil)
    {
        return
    }
    // Lock FS
    ns.lockVolume(state.ConfigPath+":fs:"+state.FsName)
    defer ns.unlockVolume(state.ConfigPath+":fs:"+state.FsName)
    // Check if NFS at this port is still mounted
    pidFile := ns.stateDir + filepath.Base(stateFile)
    pidFile = pidFile[0:len(pidFile)-5] + ".pid"
    if (len(activeNfs[state.Port]) == 0)
    {
        // this is a stale state file, remove it
        klog.Warningf("state file %v contains stale mount at port %d, removing it", stateFile, state.Port)
        ns.stopNFS(stateFile, pidFile)
        return
    }
    // Check PID file
    exists := false
    proc, err := findByPidFile(pidFile)
    if (err == nil)
    {
        exists = proc.Signal(syscall.Signal(0)) == nil
    }
    if (!exists)
    {
        // Restart vitastor-nfs server
        klog.Warningf("restarting NFS server for FS %v at port %v", state.FsName, state.Port)
        _, _, err := system(
            "/usr/bin/vitastor-nfs", "start",
            "--pidfile", pidFile,
            "--bind", "127.0.0.1",
            "--port", fmt.Sprintf("%d", state.Port),
            "--fs", state.FsName,
            "--pool", state.Pool,
            "--portmap", "0",
        )
        if (err != nil)
        {
            klog.Warningf("failed to restart NFS server for FS %v: %v", state.FsName, err)
        }
    }
}

// NodeStageVolume mounts the volume to a staging path on the node.
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error)
{
    klog.Infof("received node stage volume request %+v", protosanitizer.StripSecrets(req))

    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    _, err = GetConnectionParams(ctxVars)
    if (err != nil)
    {
        return nil, err
    }
    volName := ctxVars["name"]

    if (ctxVars["vitastorfs"] != "")
    {
        return &csi.NodeStageVolumeResponse{}, nil
    }

    ns.lockVolume(ctxVars["configPath"]+":block:"+volName)
    defer ns.unlockVolume(ctxVars["configPath"]+":block:"+volName)

    targetPath := req.GetStagingTargetPath()
    isBlock := req.GetVolumeCapability().GetBlock() != nil

    // Check that it's not already mounted
    notmnt, err := mount.IsNotMountPoint(ns.mounter, targetPath)
    if (err == nil)
    {
        if (!notmnt)
        {
            klog.Errorf("target path %s is already mounted", targetPath)
            return nil, fmt.Errorf("target path %s is already mounted", targetPath)
        }
        var finfo os.FileInfo
        finfo, err = os.Stat(targetPath)
        if (err != nil)
        {
            klog.Errorf("failed to stat %s: %v", targetPath, err)
            return nil, err
        }
        if (finfo.IsDir() != (!isBlock))
        {
            err = os.Remove(targetPath)
            if (err != nil)
            {
                klog.Errorf("failed to remove %s (to recreate it with correct type): %v", targetPath, err)
                return nil, err
            }
            err = os.ErrNotExist
        }
    }
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

    var devicePath, vdpaId string
    if (!ns.useVduse)
    {
        devicePath, err = mapNbd(volName, ctxVars, false)
    }
    else
    {
        devicePath, vdpaId, err = mapVduse(ns.stateDir, volName, ctxVars, false)
    }
    if (err != nil)
    {
        return nil, err
    }

    diskMounter := &mount.SafeFormatAndMount{Interface: ns.mounter, Exec: utilexec.New()}
    if (isBlock)
    {
        klog.Infof("bind-mounting %s to %s", devicePath, targetPath)
        err = diskMounter.Mount(devicePath, targetPath, "", []string{"bind"})
    }
    else
    {
        // Check existing format
        var existingFormat string
        existingFormat, err = diskMounter.GetDiskFormat(devicePath)
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
            switch fsType
            {
                case "ext4":
                    args := []string{"-m0", "-Enodiscard,lazy_itable_init=1,lazy_journal_init=1", devicePath}
                    _, err = systemCombined("mkfs.ext4", args...)
                case "xfs":
                    _, err = systemCombined("mkfs.xfs", "-K", devicePath)
            }
            if (err != nil)
            {
                goto unmap
            }
        }

        klog.Infof("formatting and mounting %s to %s with FS %s, options: %v", devicePath, targetPath, fsType, opt)
        err = diskMounter.FormatAndMount(devicePath, targetPath, fsType, opt)
        if (err == nil)
        {
            klog.Infof("successfully mounted %s to %s", devicePath, targetPath)
        }

        // Try to run online resize on mount.
        // FIXME: Implement online resize. It requires online resize support in vitastor-nbd.
        if (err == nil && existingFormat != "" && !readOnly)
        {
            switch (fsType)
            {
                case "ext4":
                    _, err = systemCombined("resize2fs", devicePath)
                case "xfs":
                    _, err = systemCombined("xfs_growfs", devicePath)
            }
            if (err != nil)
            {
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
    return &csi.NodeStageVolumeResponse{}, nil

unmap:
    if (!ns.useVduse || len(devicePath) >= 8 && devicePath[0:8] == "/dev/nbd")
    {
        unmapNbd(devicePath)
    }
    else
    {
        unmapVduseById(ns.stateDir, vdpaId)
    }
    return nil, err
}

// NodeUnstageVolume unstages the volume from the staging path
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error)
{
    klog.Infof("received node unstage volume request %+v", protosanitizer.StripSecrets(req))

    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    if (ctxVars["vitastorfs"] != "")
    {
        return &csi.NodeUnstageVolumeResponse{}, nil
    }

    ns.lockVolume(ctxVars["configPath"]+":block:"+volName)
    defer ns.unlockVolume(ctxVars["configPath"]+":block:"+volName)

    targetPath := req.GetStagingTargetPath()
    devicePath, err := GetDeviceNameFromMount(targetPath)
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
        return &csi.NodeUnstageVolumeResponse{}, nil
    }

    refList, err := ns.mounter.GetMountRefs(targetPath)
    if (err != nil)
    {
        return nil, err
    }
    if (len(refList) > 0)
    {
        klog.Warningf("%s is still referenced: %v", targetPath, refList)
    }

    // unmount
    err = mount.CleanupMountPoint(targetPath, ns.mounter, false)
    if (err != nil)
    {
        return nil, err
    }

    // unmap device
    if (len(refList) == 0)
    {
        if (!ns.useVduse)
        {
            unmapNbd(devicePath)
        }
        else
        {
            unmapVduse(ns.stateDir, devicePath)
        }
    }

    return &csi.NodeUnstageVolumeResponse{}, nil
}

// Mount or check if NFS is already mounted
func (ns *NodeServer) mountNFS(ctxVars map[string]string) (string, error)
{
    sum := sha1.Sum([]byte(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"]))
    nfsHash := hex.EncodeToString(sum[:])
    stateFile := ns.stateDir+"vitastor-nfs-"+nfsHash+".json"
    pidFile := ns.stateDir+"vitastor-nfs-"+nfsHash+".pid"
    mountPath := ns.nfsStageDir+"/"+nfsHash
    state, err := ns.readNfsState(stateFile, true)
    if (state != nil)
    {
        return state.Path, nil
    }
    if (err != nil)
    {
        return "", err
    }
    err = os.MkdirAll(mountPath, 0777)
    if (err != nil)
    {
        return "", err
    }
    // Create a new mount
    state = &NfsState{
        ConfigPath: ctxVars["configPath"],
        FsName:     ctxVars["vitastorfs"],
        Pool:       ctxVars["pool"],
        Path:       mountPath,
    }
    klog.Infof("starting new NFS server for FS %v", state.FsName)
    stdout, _, err := system(
        "/usr/bin/vitastor-nfs", "start",
        "--pidfile", pidFile,
        "--bind", "127.0.0.1",
        "--port", "auto",
        "--fs", state.FsName,
        "--pool", state.Pool,
        "--portmap", "0",
    )
    if (err != nil)
    {
        return "", err
    }
    match := regexp.MustCompile("Port: (\\d+)").FindStringSubmatch(string(stdout))
    if (match == nil)
    {
        klog.Errorf("failed to find port in vitastor-nfs output: %v", string(stdout))
        ns.stopNFS(stateFile, pidFile)
        return "", fmt.Errorf("failed to find port in vitastor-nfs output (bad vitastor-nfs version?)")
    }
    port, _ := strconv.ParseUint(match[1], 0, 16)
    state.Port = int(port)
    // Write state file
    stateJSON, _ := json.Marshal(state)
    err = os.WriteFile(stateFile, stateJSON, 0600)
    if (err != nil)
    {
        klog.Errorf("failed to write state file %v", stateFile)
        ns.stopNFS(stateFile, pidFile)
        return "", err
    }
    // Mount NFS
    _, _, err = system(
        "mount", "-t", "nfs", "127.0.0.1:/", state.Path,
        "-o", fmt.Sprintf("port=%d,mountport=%d,nfsvers=3,soft,nolock,tcp", port, port),
    )
    if (err != nil)
    {
        ns.stopNFS(stateFile, pidFile)
        return "", err
    }
    return state.Path, nil
}

// Mount or check if NFS is already mounted
func (ns *NodeServer) checkStopNFS(ctxVars map[string]string)
{
    sum := sha1.Sum([]byte(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"]))
    nfsHash := hex.EncodeToString(sum[:])
    stateFile := ns.stateDir+"vitastor-nfs-"+nfsHash+".json"
    pidFile := ns.stateDir+"vitastor-nfs-"+nfsHash+".pid"
    mountPath := ns.nfsStageDir+"/"+nfsHash
    state, err := ns.readNfsState(stateFile, true)
    if (state == nil)
    {
        return
    }
    activeNFS, err := ns.listActiveNFS()
    if (err != nil)
    {
        return
    }
    if (len(activeNFS[state.Port]) > 0)
    {
        return
    }
    // All volume mounts are detached, unmount the root mount and kill the server
    err = mount.CleanupMountPoint(mountPath, ns.mounter, false)
    if (err != nil)
    {
        klog.Errorf("failed to unmount %v: %v", mountPath, err)
        return
    }
    ns.stopNFS(stateFile, pidFile)
}

func (ns *NodeServer) stopNFS(stateFile, pidFile string)
{
    err := killByPidFile(pidFile)
    if (err != nil)
    {
        klog.Errorf("failed to kill process with pid from %v: %v", pidFile, err)
    }
    os.Remove(pidFile)
    os.Remove(stateFile)
}

func (ns *NodeServer) listActiveNFS() (map[int][]string, error)
{
    mounts, err := mount.ParseMountInfo("/proc/self/mountinfo")
    if (err != nil)
    {
        klog.Errorf("failed to list mounts: %v", err)
        return nil, err
    }
    activeNFS := make(map[int][]string)
    for _, mount := range mounts
    {
        // Volume mounts always refer to subpaths
        if (mount.FsType == "nfs" && mount.Root != "/")
        {
            for _, opt := range mount.MountOptions
            {
                if (strings.HasPrefix(opt, "port="))
                {
                    port64, err := strconv.ParseUint(opt[5:], 10, 16)
                    if (err == nil)
                    {
                        activeNFS[int(port64)] = append(activeNFS[int(port64)], mount.MountPoint)
                    }
                }
            }
        }
    }
    return activeNFS, nil
}

// NodePublishVolume mounts the volume mounted to the staging path to the target path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error)
{
    klog.Infof("received node publish volume request %+v", protosanitizer.StripSecrets(req))

    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    _, err = GetConnectionParams(ctxVars)
    if (err != nil)
    {
        return nil, err
    }
    volName := ctxVars["name"]

    if (ctxVars["vitastorfs"] != "")
    {
        ns.lockVolume(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"])
        defer ns.unlockVolume(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"])
    }
    else
    {
        ns.lockVolume(ctxVars["configPath"]+":block:"+volName)
        defer ns.unlockVolume(ctxVars["configPath"]+":block:"+volName)
    }

    stagingTargetPath := req.GetStagingTargetPath()
    targetPath := req.GetTargetPath()
    isBlock := req.GetVolumeCapability().GetBlock() != nil

    if (ctxVars["vitastorfs"] == "")
    {
        // Check that stagingTargetPath is mounted
        notmnt, err := mount.IsNotMountPoint(ns.mounter, stagingTargetPath)
        if (err != nil)
        {
            klog.Errorf("staging path %v is not mounted: %w", stagingTargetPath, err)
            return nil, fmt.Errorf("staging path %v is not mounted: %w", stagingTargetPath, err)
        }
        else if (notmnt)
        {
            klog.Errorf("staging path %v is not mounted", stagingTargetPath)
            return nil, fmt.Errorf("staging path %v is not mounted", stagingTargetPath)
        }
    }

    // Check that targetPath is not already mounted
    notmnt, err := mount.IsNotMountPoint(ns.mounter, targetPath)
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
    else if (!notmnt)
    {
        klog.Errorf("target path %s is already mounted", targetPath)
        return nil, fmt.Errorf("target path %s is already mounted", targetPath)
    }

    if (ctxVars["vitastorfs"] != "")
    {
        nfspath, err := ns.mountNFS(ctxVars)
        if (err != nil)
        {
            ns.checkStopNFS(ctxVars)
            return nil, err
        }
        // volName should include prefix
        stagingTargetPath = nfspath+"/"+volName
        err = os.MkdirAll(stagingTargetPath, 0777)
        if (err != nil && !os.IsExist(err))
        {
            ns.checkStopNFS(ctxVars)
            return nil, err
        }
    }

    execArgs := []string{"--bind", stagingTargetPath, targetPath}
    if (req.GetReadonly())
    {
        execArgs = append(execArgs, "-o", "ro")
    }
    cmd := exec.Command("mount", execArgs...)
    cmd.Stderr = os.Stderr
    klog.Infof("binding volume %v (%v) from %v to %v", volName, ctxVars["configPath"], stagingTargetPath, targetPath)
    out, err := cmd.Output()
    if (err != nil)
    {
        if (ctxVars["vitastorfs"] != "")
        {
            ns.checkStopNFS(ctxVars)
        }
        return nil, fmt.Errorf("Error running mount %v: %s", strings.Join(execArgs, " "), out)
    }

    return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error)
{
    klog.Infof("received node unpublish volume request %+v", protosanitizer.StripSecrets(req))

    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    if (ctxVars["vitastorfs"] != "")
    {
        ns.lockVolume(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"])
        defer ns.unlockVolume(ctxVars["configPath"]+":fs:"+ctxVars["vitastorfs"])
    }
    else
    {
        ns.lockVolume(ctxVars["configPath"]+":block:"+volName)
        defer ns.unlockVolume(ctxVars["configPath"]+":block:"+volName)
    }

    targetPath := req.GetTargetPath()
    devicePath, err := GetDeviceNameFromMount(targetPath)
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

    if (ctxVars["vitastorfs"] != "")
    {
        ns.checkStopNFS(ctxVars)
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
    return &csi.NodeGetCapabilitiesResponse{
        Capabilities: []*csi.NodeServiceCapability{
            &csi.NodeServiceCapability{
                Type: &csi.NodeServiceCapability_Rpc{
                    Rpc: &csi.NodeServiceCapability_RPC{
                        Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
                    },
                },
            },
        },
    }, nil
}

// NodeGetInfo returns NodeGetInfoResponse for CO.
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error)
{
    klog.Infof("received node get info request %+v", protosanitizer.StripSecrets(req))
    return &csi.NodeGetInfoResponse{
        NodeId: ns.NodeID,
    }, nil
}
