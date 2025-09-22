// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "bytes"
    "errors"
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "syscall"

    "k8s.io/klog"
    "k8s.io/utils/mount"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

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
    else
    {
        klog.Infof("VDUSE support enabled successfully")
    }
    return vduse
}

func mapNbd(volName string, ctxVars map[string]string, readonly bool) (string, error)
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
    klog.Infof("Attached volume %s via NBD as %s", volName, dev)
    return dev, err
}

func unmapNbd(devicePath string)
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
    klog.Infof("killing process with PID from file %s", pidFile)
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

func mapVduse(stateDir string, volName string, ctxVars map[string]string, readonly bool) (string, string, error)
{
    // Generate state file
    stateFd, err := os.CreateTemp(stateDir, "vitastor-vduse-*.json")
    if (err != nil)
    {
        return "", "", err
    }
    stateFile := stateFd.Name()
    stateFd.Close()
    vdpaId := filepath.Base(stateFile)
    vdpaId = vdpaId[0:len(vdpaId)-5] // remove ".json"
    pidFile := stateDir + vdpaId + ".pid"
    // Map VDUSE device via qemu-storage-daemon
    err = startStorageDaemon(vdpaId, volName, pidFile, ctxVars["configPath"], readonly)
    if (err == nil)
    {
        // Add device to VDPA bus
        _, _, err = system("/sbin/vdpa", "-j", "dev", "add", "name", vdpaId, "mgmtdev", "vduse")
        if (err == nil)
        {
            // Find block device name
            var matches []string
            matches, err = filepath.Glob("/sys/bus/vdpa/devices/"+vdpaId+"/virtio*/block/*")
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
                        klog.Infof("Attached volume %s via VDUSE as %s (VDPA ID %s)", volName, blockdev, vdpaId)
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

func unmapVduse(stateDir, devicePath string)
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
    unmapVduseById(stateDir, vdpaId)
}

func unmapVduseById(stateDir, vdpaId string)
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
    stateFile := stateDir + vdpaId + ".json"
    os.Remove(stateFile)
    pidFile := stateDir + vdpaId + ".pid"
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

func system(program string, args ...string) ([]byte, []byte, error)
{
    klog.Infof("Running "+program+" "+strings.Join(args, " "))
    c := exec.Command(program, args...)
    var stdout, stderr bytes.Buffer
    c.Stdout, c.Stderr = &stdout, &stderr
    err := c.Run()
    if (err != nil)
    {
        stdoutStr, stderrStr := string(stdout.Bytes()), string(stderr.Bytes())
        klog.Errorf(program+" "+strings.Join(args, " ")+" failed: %s\nOutput:\n%s", err, stdoutStr+stderrStr)
        return nil, nil, status.Error(codes.Internal, stdoutStr+stderrStr+" (status "+err.Error()+")")
    }
    return stdout.Bytes(), stderr.Bytes(), nil
}

func systemCombined(program string, args ...string) ([]byte, error)
{
    klog.Infof("Running "+program+" "+strings.Join(args, " "))
    c := exec.Command(program, args...)
    var out bytes.Buffer
    c.Stdout, c.Stderr = &out, &out
    err := c.Run()
    if (err != nil)
    {
        outStr := string(out.Bytes())
        klog.Errorf(program+" "+strings.Join(args, " ")+" failed: %s, status %s\n", outStr, err)
        return nil, status.Error(codes.Internal, outStr+" (status "+err.Error()+")")
    }
    return out.Bytes(), nil
}

func GetDeviceNameFromMount(mountPath string) (string, error)
{
    // Use /proc/self/mountinfo to correctly parse bind mounts for block device files
    mps, err := mount.ParseMountInfo("/proc/self/mountinfo")
    if (err != nil)
    {
        return "", err
    }

    slTarget, err := filepath.EvalSymlinks(mountPath)
    if (err != nil)
    {
        slTarget = mountPath
    }

    device := ""
    for _, mp := range mps
    {
        if (mp.MountPoint == slTarget)
        {
            device = mp.Source
            if (device[0] != '/' && mp.Root != "/")
            {
                // Handle {Source=udev Root=/vdb MountPoint=/var/lib/kubelet/tralaleylo/tralala}
                for _, other := range mps
                {
                    if (other.Root == "/" && other.Source == mp.Source)
                    {
                        device = other.MountPoint + mp.Root
                        break
                    }
                }
            }
            break
        }
    }

    return device, nil
}
