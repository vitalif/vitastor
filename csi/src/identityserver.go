// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "context"

    "github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
    "k8s.io/klog"

    "github.com/container-storage-interface/spec/lib/go/csi"
)

// IdentityServer struct of Vitastor CSI driver with supported methods of CSI identity server spec.
type IdentityServer struct
{
    *Driver
}

// NewIdentityServer create new instance identity
func NewIdentityServer(driver *Driver) *IdentityServer
{
    return &IdentityServer{
        Driver: driver,
    }
}

// GetPluginInfo returns metadata of the plugin
func (is *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error)
{
    klog.Infof("received identity plugin info request %+v", protosanitizer.StripSecrets(req))
    return &csi.GetPluginInfoResponse{
        Name:          vitastorCSIDriverName,
        VendorVersion: vitastorCSIDriverVersion,
    }, nil
}

// GetPluginCapabilities returns available capabilities of the plugin
func (is *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error)
{
    klog.Infof("received identity plugin capabilities request %+v", protosanitizer.StripSecrets(req))
    return &csi.GetPluginCapabilitiesResponse{
        Capabilities: []*csi.PluginCapability{
            {
                Type: &csi.PluginCapability_Service_{
                    Service: &csi.PluginCapability_Service{
                        Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
                    },
                },
            },
        },
    }, nil
}

// Probe returns the health and readiness of the plugin
func (is *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error)
{
    return &csi.ProbeResponse{}, nil
}
