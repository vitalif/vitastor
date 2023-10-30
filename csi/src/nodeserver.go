// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"
    "os"
    "os/exec"
    "encoding/json"
    "strings"
    "bytes"

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
    mounter mount.Interface
}

// NewNodeServer create new instance node
func NewNodeServer(driver *Driver) *NodeServer
{
    return &NodeServer{
        Driver: driver,
        mounter: mount.New(""),
    }
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
                    return nil, status.Error(codes.Internal, err.Error())
                }
                err = pathFile.Close()
                if (err != nil)
                {
                    klog.Errorf("failed to close %s with error: %v", targetPath, err)
                    return nil, status.Error(codes.Internal, err.Error())
                }
            }
            else
            {
                err := os.MkdirAll(targetPath, 0777)
                if (err != nil)
                {
                    klog.Errorf("failed to create fs mount target %s with error: %v", targetPath, err)
                    return nil, status.Error(codes.Internal, err.Error())
                }
            }
        }
        else
        {
            return nil, status.Error(codes.Internal, err.Error())
        }
    }

    ctxVars := make(map[string]string)
    err = json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    _, etcdUrl, etcdPrefix := GetConnectionParams(ctxVars)
    if (len(etcdUrl) == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "no etcdUrl in storage class configuration and no etcd_address in vitastor.conf")
    }

    // Map NBD device
    // FIXME: Check if already mapped
    args := []string{
        "map", "--etcd_address", strings.Join(etcdUrl, ","),
        "--etcd_prefix", etcdPrefix,
        "--image", volName,
    };
    if (ctxVars["configPath"] != "")
    {
        args = append(args, "--config_path", ctxVars["configPath"])
    }
    if (req.GetReadonly())
    {
        args = append(args, "--readonly", "1")
    }
    c := exec.Command("/usr/bin/vitastor-nbd", args...)
    var stdout, stderr bytes.Buffer
    c.Stdout, c.Stderr = &stdout, &stderr
    err = c.Run()
    stdoutStr, stderrStr := string(stdout.Bytes()), string(stderr.Bytes())
    if (err != nil)
    {
        klog.Errorf("vitastor-nbd map failed: %s, status %s\n", stdoutStr+stderrStr, err)
        return nil, status.Error(codes.Internal, stdoutStr+stderrStr+" (status "+err.Error()+")")
    }
    devicePath := strings.TrimSpace(stdoutStr)

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
    // unmap NBD device
    unmapOut, unmapErr := exec.Command("/usr/bin/vitastor-nbd", "unmap", devicePath).CombinedOutput()
    if (unmapErr != nil)
    {
        klog.Errorf("failed to unmap NBD device %s: %s, error: %v", devicePath, unmapOut, unmapErr)
    }
    return nil, status.Error(codes.Internal, err.Error())
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
        return nil, status.Error(codes.Internal, err.Error())
    }
    if (devicePath == "")
    {
        return nil, status.Error(codes.NotFound, "Volume not mounted")
    }
    // unmount
    err = mount.CleanupMountPoint(targetPath, ns.mounter, false)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, err.Error())
    }
    // unmap NBD device
    if (refCount == 1)
    {
        unmapOut, unmapErr := exec.Command("/usr/bin/vitastor-nbd", "unmap", devicePath).CombinedOutput()
        if (unmapErr != nil)
        {
            klog.Errorf("failed to unmap NBD device %s: %s, error: %v", devicePath, unmapOut, unmapErr)
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
