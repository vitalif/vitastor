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
    "fmt"
    "os"
    "os/exec"
    "io/ioutil"

    "github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
    "k8s.io/klog"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    "go.etcd.io/etcd/clientv3"

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

    // FIXME: The following should PROBABLY be implemented externally in a management tool

    ctxVars, etcdUrl, etcdPrefix := GetConnectionParams(req.Parameters)
    if (len(etcdUrl) == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "no etcdUrl in storage class configuration and no etcd_address in vitastor.conf")
    }

    // Connect to etcd
    cli, err := clientv3.New(clientv3.Config{
        DialTimeout: ETCD_TIMEOUT,
        Endpoints: etcdUrl,
    })
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "failed to connect to etcd at "+strings.Join(etcdUrl, ",")+": "+err.Error())
    }
    defer cli.Close()

    var imageId uint64 = 0
    for
    {
        // Check if the image exists
        ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
        resp, err := cli.Get(ctx, etcdPrefix+"/index/image/"+volName)
        cancel()
        if (err != nil)
        {
            return nil, status.Error(codes.Internal, "failed to read key from etcd: "+err.Error())
        }
        if (len(resp.Kvs) > 0)
        {
            kv := resp.Kvs[0]
            var v InodeIndex
            err := json.Unmarshal(kv.Value, &v)
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "invalid /index/image/"+volName+" key in etcd: "+err.Error())
            }
            poolId = v.PoolId
            imageId = v.Id
            inodeCfgKey := fmt.Sprintf("/config/inode/%d/%d", poolId, imageId)
            ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
            resp, err := cli.Get(ctx, etcdPrefix+inodeCfgKey)
            cancel()
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "failed to read key from etcd: "+err.Error())
            }
            if (len(resp.Kvs) == 0)
            {
                return nil, status.Error(codes.Internal, "missing "+inodeCfgKey+" key in etcd")
            }
            var inodeCfg InodeConfig
            err = json.Unmarshal(resp.Kvs[0].Value, &inodeCfg)
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "invalid "+inodeCfgKey+" key in etcd: "+err.Error())
            }
            if (inodeCfg.Size < uint64(volSize))
            {
                return nil, status.Error(codes.Internal, "image "+volName+" is already created, but size is less than expected")
            }
        }
        else
        {
            // Find a free ID
            // Create image metadata in a transaction verifying that the image doesn't exist yet AND ID is still free
            maxIdKey := fmt.Sprintf("%s/index/maxid/%d", etcdPrefix, poolId)
            ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
            resp, err := cli.Get(ctx, maxIdKey)
            cancel()
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "failed to read key from etcd: "+err.Error())
            }
            var modRev int64
            var nextId uint64
            if (len(resp.Kvs) > 0)
            {
                var err error
                nextId, err = strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
                if (err != nil)
                {
                    return nil, status.Error(codes.Internal, maxIdKey+" contains invalid ID")
                }
                modRev = resp.Kvs[0].ModRevision
                nextId++
            }
            else
            {
                nextId = 1
            }
            inodeIdxJson, _ := json.Marshal(InodeIndex{
                Id: nextId,
                PoolId: poolId,
            })
            inodeCfgJson, _ := json.Marshal(InodeConfig{
                Name: volName,
                Size: uint64(volSize),
            })
            ctx, cancel = context.WithTimeout(context.Background(), ETCD_TIMEOUT)
            txnResp, err := cli.Txn(ctx).If(
                clientv3.Compare(clientv3.ModRevision(fmt.Sprintf("%s/index/maxid/%d", etcdPrefix, poolId)), "=", modRev),
                clientv3.Compare(clientv3.CreateRevision(fmt.Sprintf("%s/index/image/%s", etcdPrefix, volName)), "=", 0),
                clientv3.Compare(clientv3.CreateRevision(fmt.Sprintf("%s/config/inode/%d/%d", etcdPrefix, poolId, nextId)), "=", 0),
            ).Then(
                clientv3.OpPut(fmt.Sprintf("%s/index/maxid/%d", etcdPrefix, poolId), fmt.Sprintf("%d", nextId)),
                clientv3.OpPut(fmt.Sprintf("%s/index/image/%s", etcdPrefix, volName), string(inodeIdxJson)),
                clientv3.OpPut(fmt.Sprintf("%s/config/inode/%d/%d", etcdPrefix, poolId, nextId), string(inodeCfgJson)),
            ).Commit()
            cancel()
            if (err != nil)
            {
                return nil, status.Error(codes.Internal, "failed to commit transaction in etcd: "+err.Error())
            }
            if (txnResp.Succeeded)
            {
                imageId = nextId
                break
            }
            // Start over if the transaction fails
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

    _, etcdUrl, etcdPrefix := GetConnectionParams(ctxVars)
    if (len(etcdUrl) == 0)
    {
        return nil, status.Error(codes.InvalidArgument, "no etcdUrl in storage class configuration and no etcd_address in vitastor.conf")
    }

    cli, err := clientv3.New(clientv3.Config{
        DialTimeout: ETCD_TIMEOUT,
        Endpoints: etcdUrl,
    })
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "failed to connect to etcd at "+strings.Join(etcdUrl, ",")+": "+err.Error())
    }
    defer cli.Close()

    // Find inode by name
    ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
    resp, err := cli.Get(ctx, etcdPrefix+"/index/image/"+volName)
    cancel()
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "failed to read key from etcd: "+err.Error())
    }
    if (len(resp.Kvs) == 0)
    {
        return nil, status.Error(codes.NotFound, "volume "+volName+" does not exist")
    }
    var idx InodeIndex
    err = json.Unmarshal(resp.Kvs[0].Value, &idx)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "invalid /index/image/"+volName+" key in etcd: "+err.Error())
    }

    // Get inode config
    inodeCfgKey := fmt.Sprintf("%s/config/inode/%d/%d", etcdPrefix, idx.PoolId, idx.Id)
    ctx, cancel = context.WithTimeout(context.Background(), ETCD_TIMEOUT)
    resp, err = cli.Get(ctx, inodeCfgKey)
    cancel()
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "failed to read key from etcd: "+err.Error())
    }
    if (len(resp.Kvs) == 0)
    {
        return nil, status.Error(codes.NotFound, "volume "+volName+" does not exist")
    }
    var inodeCfg InodeConfig
    err = json.Unmarshal(resp.Kvs[0].Value, &inodeCfg)
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "invalid "+inodeCfgKey+" key in etcd: "+err.Error())
    }

    // Delete inode data by invoking vitastor-rm
    args := []string{
        "--etcd_address", strings.Join(etcdUrl, ","),
        "--pool", fmt.Sprintf("%d", idx.PoolId),
        "--inode", fmt.Sprintf("%d", idx.Id),
    }
    if (ctxVars["configPath"] != "")
    {
        args = append(args, "--config_path", ctxVars["configPath"])
    }
    c := exec.Command("/usr/bin/vitastor-rm", args...)
    var stderr bytes.Buffer
    c.Stdout = nil
    c.Stderr = &stderr
    err = c.Run()
    stderrStr := string(stderr.Bytes())
    if (err != nil)
    {
        klog.Errorf("vitastor-rm failed: %s, status %s\n", stderrStr, err)
        return nil, status.Error(codes.Internal, stderrStr+" (status "+err.Error()+")")
    }

    // Delete inode config in etcd
    ctx, cancel = context.WithTimeout(context.Background(), ETCD_TIMEOUT)
    txnResp, err := cli.Txn(ctx).Then(
        clientv3.OpDelete(fmt.Sprintf("%s/index/image/%s", etcdPrefix, volName)),
        clientv3.OpDelete(fmt.Sprintf("%s/config/inode/%d/%d", etcdPrefix, idx.PoolId, idx.Id)),
    ).Commit()
    cancel()
    if (err != nil)
    {
        return nil, status.Error(codes.Internal, "failed to delete keys in etcd: "+err.Error())
    }
    if (!txnResp.Succeeded)
    {
        return nil, status.Error(codes.Internal, "failed to delete keys in etcd: transaction failed")
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
