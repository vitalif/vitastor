// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

import (
    "k8s.io/klog"
)

type Driver struct
{
    *Config
}

// NewDriver create new instance driver
func NewDriver(config *Config) (*Driver, error)
{
    if (config == nil)
    {
        klog.Errorf("Vitastor CSI driver initialization failed")
        return nil, nil
    }
    driver := &Driver{
        Config: config,
    }
    klog.Infof("Vitastor CSI driver initialized")
    return driver, nil
}

// Start server
func (driver *Driver) Run()
{
    server := NewNonBlockingGRPCServer()
    server.Start(driver.Endpoint, NewIdentityServer(driver), NewControllerServer(driver), NewNodeServer(driver))
    server.Wait()
}
