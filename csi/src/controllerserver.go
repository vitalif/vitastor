// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"
    "encoding/json"
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

func GetConnectionParams(params map[string]string) (map[string]string, []string, string)
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
    if configFD, err := os.Open(configPath); err == nil
    {
        defer configFD.Close()
        data, _ := ioutil.ReadAll(configFD)
        json.Unmarshal(data, &config)
    }
    // Try to load prefix & etcd URL from the config
    var etcdUrl []string
    if (params["etcdUrl"] != "")
    {
        ctxVars["etcdUrl"] = params["etcdUrl"]
        etcdUrl = strings.Split(params["etcdUrl"], ",")
    }
    if (len(etcdUrl) == 0)
    {
        switch config["etcd_address"].(type)
        {
        case string:
            etcdUrl = strings.Split(config["etcd_address"].(string), ",")
        case []string:
            etcdUrl = config["etcd_address"].([]string)
        }
    }
    etcdPrefix := params["etcdPrefix"]
    if (etcdPrefix == "")
    {
        etcdPrefix, _ = config["etcd_prefix"].(string)
        if (etcdPrefix == "")
        {
            etcdPrefix = "/vitastor"
        }
    }
    else
    {
        ctxVars["etcdPrefix"] = etcdPrefix
    }
    return ctxVars, etcdUrl, etcdPrefix
}

func invokeCLI(ctxVars map[string]string, args []string) ([]byte, error)
{
    if (ctxVars["etcdUrl"] != "")
    {
        args = append(args, "--etcd_address", ctxVars["etcdUrl"])
    }
    if (ctxVars["etcdPrefix"] != "")
    {
        args = append(args, "--etcd_prefix", ctxVars["etcdPrefix"])
    }
    if (ctxVars["configPath"] != "")
    {
        args = append(args, "--config_path", ctxVars["configPath"])
    }
    c := exec.Command("/usr/bin/vitastor-cli", args...)
    var stdout, stderr bytes.Buffer
    c.Stdout = &stdout
    c.Stderr = &stderr
    err := c.Run()
    stderrStr := string(stderr.Bytes())
    if (err != nil)
    {
        klog.Errorf("vitastor-cli %s failed: %s, status %s\n", strings.Join(args, " "), stderrStr, err)
        return nil, status.Error(codes.Internal, stderrStr+" (status "+err.Error()+")")
    }
    return stdout.Bytes(), nil
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

    ctxVars, etcdUrl, _ := GetConnectionParams(req.Parameters)
    if (len(etcdUrl) == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "no etcdUrl in storage class configuration and no etcd_address in vitastor.conf")
    }

    // Create image using vitastor-cli
    _, err := invokeCLI(ctxVars, []string{ "create", volName, "-s", string(volSize), "--pool", string(poolId) })
    if (err != nil)
    {
        if (strings.Index(err.Error(), "already exists") > 0)
        {
            stat, err := invokeCLI(ctxVars, []string{ "ls", "--json", volName })
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
            if (len(inodeCfg) == 0)
            {
                return nil, status.Error(codes.Internal, "vitastor-cli create said that image already exists, but ls can't find it")
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

    ctxVars := make(map[string]string)
    err := json.Unmarshal([]byte(req.VolumeId), &ctxVars)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "volume ID not in JSON format")
    }
    volName := ctxVars["name"]

    ctxVars, _, _ = GetConnectionParams(ctxVars)

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
    } {
        controllerServerCapabilities = append(controllerServerCapabilities, functionControllerServerCapabilities(capability))
    }

    return &csi.ControllerGetCapabilitiesResponse{
        Capabilities: controllerServerCapabilities,
    }, nil
}

// CreateSnapshot create snapshot of an existing PV
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// DeleteSnapshot delete provided snapshot of a PV
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ListSnapshots list the snapshots of a PV
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ControllerExpandVolume resizes a volume
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetVolume get volume info
func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error)
{
    return nil, status.Error(codes.Unimplemented, "")
}
