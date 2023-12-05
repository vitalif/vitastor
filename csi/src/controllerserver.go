// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"
    "bytes"
    "strconv"
    "time"
    "os"
    "os/exec"
    "io/ioutil"

    "github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
    "k8s.io/klog"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/timestamppb"

    "github.com/container-storage-interface/spec/lib/go/csi"
)

const (
    KB int64 = 1024
    MB int64 = 1024 * KB
    GB int64 = 1024 * MB
    TB int64 = 1024 * GB
    ETCD_TIMEOUT time.Duration = 15*time.Second
)

type InodeIndex struct
{
    Id uint64 `json:"id"`
    PoolId uint64 `json:"pool_id"`
}

type InodeConfig struct
{
    Name string `json:"name"`
    Size uint64 `json:"size,omitempty"`
    ParentPool uint64 `json:"parent_pool,omitempty"`
    ParentId uint64 `json:"parent_id,omitempty"`
    Readonly bool `json:"readonly,omitempty"`
    CreateTs uint64 `json:"create_ts,omitempty"`
}

type ControllerServer struct
{
    *Driver
}

// NewControllerServer create new instance controller
func NewControllerServer(driver *Driver) *ControllerServer
{
    return &ControllerServer{
        Driver: driver,
    }
}

func GetConnectionParams(params map[string]string) (map[string]string, error)
{
    ctxVars := make(map[string]string)
    configPath := params["configPath"]
    if (configPath == "")
    {
        configPath = "/etc/vitastor/vitastor.conf"
    }
    else
    {
        ctxVars["configPath"] = configPath
    }
    config := make(map[string]interface{})
    configFD, err := os.Open(configPath)
    if (err != nil)
    {
        return nil, err
    }
    defer configFD.Close()
    data, _ := ioutil.ReadAll(configFD)
    json.Unmarshal(data, &config)
    // Check etcd URL in the config, but do not use the explicit etcdUrl
    // parameter for CLI calls, otherwise users won't be able to later
    // change them - storage class parameters are saved in volume IDs
    var etcdUrl []string
    switch config["etcd_address"].(type)
    {
    case string:
        url := strings.TrimSpace(config["etcd_address"].(string))
        if (url != "")
        {
            etcdUrl = strings.Split(url, ",")
        }
    case []string:
        etcdUrl = config["etcd_address"].([]string)
    case []interface{}:
        for _, url := range config["etcd_address"].([]interface{})
        {
            s, ok := url.(string)
            if (ok)
            {
                etcdUrl = append(etcdUrl, s)
            }
        }
    }
    if (len(etcdUrl) == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "etcd_address is missing in "+configPath)
    }
    return ctxVars, nil
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
        klog.Errorf(program+" "+strings.Join(args, " ")+" failed: %s, status %s\n", stdoutStr+stderrStr, err)
        return nil, nil, status.Error(codes.Internal, stdoutStr+stderrStr+" (status "+err.Error()+")")
    }
    return stdout.Bytes(), stderr.Bytes(), nil
}

func invokeCLI(ctxVars map[string]string, args []string) ([]byte, error)
{
    if (ctxVars["configPath"] != "")
    {
        args = append(args, "--config_path", ctxVars["configPath"])
    }
    stdout, _, err := system("/usr/bin/vitastor-cli", args...)
    return stdout, err
}

// Create the volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error)
{
    klog.Infof("received controller create volume request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
    }
    if (req.GetName() == "")
    {
        return nil, status.Error(codes.InvalidArgument, "name is a required field")
    }
    volumeCapabilities := req.GetVolumeCapabilities()
    if (volumeCapabilities == nil)
    {
        return nil, status.Error(codes.InvalidArgument, "volume capabilities is a required field")
    }

    etcdVolumePrefix := req.Parameters["etcdVolumePrefix"]
    poolId, _ := strconv.ParseUint(req.Parameters["poolId"], 10, 64)
    if (poolId == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "poolId is missing in storage class configuration")
    }

    volName := etcdVolumePrefix + req.GetName()
    volSize := 1 * GB
    if capRange := req.GetCapacityRange(); capRange != nil
    {
        volSize = ((capRange.GetRequiredBytes() + MB - 1) / MB) * MB
    }

    ctxVars, err := GetConnectionParams(req.Parameters)
    if (err != nil)
    {
        return nil, err
    }

    args := []string{ "create", volName, "-s", fmt.Sprintf("%v", volSize), "--pool", fmt.Sprintf("%v", poolId) }

    // Support creation from snapshot
    var src *csi.VolumeContentSource
    if (req.VolumeContentSource.GetSnapshot() != nil)
    {
        snapId := req.VolumeContentSource.GetSnapshot().GetSnapshotId()
        if (snapId != "")
        {
            snapVars := make(map[string]string)
            err := json.Unmarshal([]byte(snapId), &snapVars)
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "volume ID not in JSON format")
            }
            args = append(args, "--parent", snapVars["name"]+"@"+snapVars["snapshot"])
            src = &csi.VolumeContentSource{
                Type: &csi.VolumeContentSource_Snapshot{
                    Snapshot: &csi.VolumeContentSource_SnapshotSource{
                        SnapshotId: snapId,
                    },
                },
            }
        }
    }

    // Create image using vitastor-cli
    _, err = invokeCLI(ctxVars, args)
    if (err != nil)
    {
        if (strings.Index(err.Error(), "already exists") > 0)
        {
            inodeCfg, err := invokeList(ctxVars, volName, true)
            if (err != nil)
            {
                return nil, err
            }
            if (inodeCfg[0].Size < uint64(volSize))
            {
                return nil, status.Error(codes.Internal, "image "+volName+" is already created, but size is less than expected")
            }
        }
        else
        {
            return nil, err
        }
    }

    ctxVars["name"] = volName
    volumeIdJson, _ := json.Marshal(ctxVars)
    return &csi.CreateVolumeResponse{
        Volume: &csi.Volume{
            // Ugly, but VolumeContext isn't passed to DeleteVolume :-(
            VolumeId: string(volumeIdJson),
            CapacityBytes: volSize,
            ContentSource: src,
        },
    }, nil
}

// DeleteVolume deletes the given volume
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error)
{
    klog.Infof("received controller delete volume request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Error(codes.InvalidArgument, "request cannot be empty")
    }

    volVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &volVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := volVars["name"]

    ctxVars, err := GetConnectionParams(volVars)
    if (err != nil)
    {
        return nil, err
    }

    _, err = invokeCLI(ctxVars, []string{ "rm", volName })
    if (err != nil)
    {
        return nil, err
    }

    return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ControllerUnpublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error)
{
    klog.Infof("received controller validate volume capability request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Errorf(codes.InvalidArgument, "request is nil")
    }
    volumeID := req.GetVolumeId()
    if (volumeID == "")
    {
        return nil, status.Error(codes.InvalidArgument, "volumeId is nil")
    }
    volumeCapabilities := req.GetVolumeCapabilities()
    if (volumeCapabilities == nil)
    {
        return nil, status.Error(codes.InvalidArgument, "volumeCapabilities is nil")
    }

    var volumeCapabilityAccessModes []*csi.VolumeCapability_AccessMode
    for _, mode := range []csi.VolumeCapability_AccessMode_Mode{
        csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
        csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
    } {
        volumeCapabilityAccessModes = append(volumeCapabilityAccessModes, &csi.VolumeCapability_AccessMode{Mode: mode})
    }

    capabilitySupport := false
    for _, capability := range volumeCapabilities
    {
        for _, volumeCapabilityAccessMode := range volumeCapabilityAccessModes
        {
            if (volumeCapabilityAccessMode.Mode == capability.AccessMode.Mode)
            {
                capabilitySupport = true
            }
        }
    }

    if (!capabilitySupport)
    {
        return nil, status.Errorf(codes.NotFound, "%v not supported", req.GetVolumeCapabilities())
    }

    return &csi.ValidateVolumeCapabilitiesResponse{
        Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
            VolumeCapabilities: req.VolumeCapabilities,
        },
    }, nil
}

// ListVolumes returns a list of volumes
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// GetCapacity returns the capacity of the storage pool
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error)
{
    functionControllerServerCapabilities := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability
    {
        return &csi.ControllerServiceCapability{
            Type: &csi.ControllerServiceCapability_Rpc{
                Rpc: &csi.ControllerServiceCapability_RPC{
                    Type: cap,
                },
            },
        }
    }

    var controllerServerCapabilities []*csi.ControllerServiceCapability
    for _, capability := range []csi.ControllerServiceCapability_RPC_Type{
        csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
        csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
        csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
        csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
        csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
        // TODO: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
    } {
        controllerServerCapabilities = append(controllerServerCapabilities, functionControllerServerCapabilities(capability))
    }

    return &csi.ControllerGetCapabilitiesResponse{
        Capabilities: controllerServerCapabilities,
    }, nil
}

func invokeList(ctxVars map[string]string, pattern string, expectExist bool) ([]InodeConfig, error)
{
    stat, err := invokeCLI(ctxVars, []string{ "ls", "--json", pattern })
    if (err != nil)
    {
        return nil, err
    }
    var inodeCfg []InodeConfig
    err = json.Unmarshal(stat, &inodeCfg)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "Invalid JSON in vitastor-cli ls: "+err.Error())
    }
    if (expectExist && len(inodeCfg) == 0)
    {
        return nil, status.Error(codes.Internal, "Can't find expected image "+pattern+" via vitastor-cli ls")
    }
    return inodeCfg, nil
}

// CreateSnapshot create snapshot of an existing PV
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error)
{
    klog.Infof("received controller create snapshot request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
    }
    if (req.SourceVolumeId == "" || req.Name == "")
    {
        return nil, status.Error(codes.InvalidArgument, "source volume ID and snapshot name are required fields")
    }

    // snapshot name
    snapName := req.Name

    // req.VolumeId is an ugly json string in our case :)
    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.SourceVolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    // Create image using vitastor-cli
    _, err = invokeCLI(ctxVars, []string{ "create", "--snapshot", snapName, volName })
    if (err != nil && strings.Index(err.Error(), "already exists") <= 0)
    {
        return nil, err
    }

    // Check created snapshot
    inodeCfg, err := invokeList(ctxVars, volName+"@"+snapName, true)
    if (err != nil)
    {
        return nil, err
    }

    // Use ugly JSON snapshot ID again, DeleteSnapshot doesn't have context :-(
    ctxVars["snapshot"] = snapName
    snapIdJson, _ := json.Marshal(ctxVars)
    return &csi.CreateSnapshotResponse{
        Snapshot: &csi.Snapshot{
            SizeBytes: int64(inodeCfg[0].Size),
            SnapshotId: string(snapIdJson),
            SourceVolumeId: req.SourceVolumeId,
            CreationTime: &timestamppb.Timestamp{ Seconds: int64(inodeCfg[0].CreateTs) },
            ReadyToUse: true,
        },
    }, nil
}

// DeleteSnapshot delete provided snapshot of a PV
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error)
{
    klog.Infof("received controller delete snapshot request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
    }
    if (req.SnapshotId == "")
    {
        return nil, status.Error(codes.InvalidArgument, "snapshot ID is a required field")
    }

    volVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.SnapshotId), &volVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "snapshot ID not in JSON format")
    }
    volName := volVars["name"]
    snapName := volVars["snapshot"]

    ctxVars, err := GetConnectionParams(volVars)
    if (err != nil)
    {
        return nil, err
    }

    _, err = invokeCLI(ctxVars, []string{ "rm", volName+"@"+snapName })
    if (err != nil)
    {
        return nil, err
    }

    return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots list the snapshots of a PV
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error)
{
    klog.Infof("received controller list snapshots request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Error(codes.InvalidArgument, "request cannot be empty")
    }

    volVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.SourceVolumeId), &volVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := volVars["name"]
    ctxVars, err := GetConnectionParams(volVars)
    if (err != nil)
    {
        return nil, err
    }

    inodeCfg, err := invokeList(ctxVars, volName+"@*", false)
    if (err != nil)
    {
        return nil, err
    }

    resp := &csi.ListSnapshotsResponse{}
    for _, ino := range inodeCfg
    {
        snapName := ino.Name[len(volName)+1:]
        if (len(req.StartingToken) > 0 && snapName < req.StartingToken)
        {
        }
        else if (req.MaxEntries == 0 || len(resp.Entries) < int(req.MaxEntries))
        {
            volVars["snapshot"] = snapName
            snapIdJson, _ := json.Marshal(volVars)
            resp.Entries = append(resp.Entries, &csi.ListSnapshotsResponse_Entry{
                Snapshot: &csi.Snapshot{
                    SizeBytes: int64(ino.Size),
                    SnapshotId: string(snapIdJson),
                    SourceVolumeId: req.SourceVolumeId,
                    CreationTime: &timestamppb.Timestamp{ Seconds: int64(ino.CreateTs) },
                    ReadyToUse: true,
                },
            })
        }
        else
        {
            resp.NextToken = snapName
            break
        }
    }

    return resp, nil
}

// ControllerExpandVolume increases the size of a volume
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error)
{
    klog.Infof("received controller expand volume request %+v", protosanitizer.StripSecrets(req))
    if (req == nil)
    {
        return nil, status.Error(codes.InvalidArgument, "request cannot be empty")
    }
    if (req.VolumeId == "" || req.CapacityRange == nil || req.CapacityRange.RequiredBytes == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "VolumeId, CapacityRange and RequiredBytes are required fields")
    }

    volVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &volVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := volVars["name"]
    ctxVars, err := GetConnectionParams(volVars)
    if (err != nil)
    {
        return nil, err
    }

    inodeCfg, err := invokeList(ctxVars, volName, true)
    if (err != nil)
    {
        return nil, err
    }

    if (req.CapacityRange.RequiredBytes > 0 && inodeCfg[0].Size < uint64(req.CapacityRange.RequiredBytes))
    {
        sz := ((req.CapacityRange.RequiredBytes+4095)/4096)*4096
        _, err := invokeCLI(ctxVars, []string{ "modify", "--inc_size", "1", "--resize", fmt.Sprintf("%d", sz), volName })
        if (err != nil)
        {
            return nil, err
        }
        inodeCfg, err = invokeList(ctxVars, volName, true)
        if (err != nil)
        {
            return nil, err
        }
    }

    return &csi.ControllerExpandVolumeResponse{
        CapacityBytes: int64(inodeCfg[0].Size),
        NodeExpansionRequired: false,
    }, nil
}

// ControllerGetVolume get volume info
func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}
