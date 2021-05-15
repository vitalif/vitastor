// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package main

import (
    "flag"
    "fmt"
    "os"
    "k8s.io/klog"
    "vitastor.io/csi/src"
)

func main()
{
    var config = vitastor.NewConfig()
    flag.StringVar(&config.Endpoint, "endpoint", "", "CSI endpoint")
    flag.StringVar(&config.NodeID, "node", "", "Node ID")
    flag.Parse()
    if (config.Endpoint == "")
    {
        config.Endpoint = os.Getenv("CSI_ENDPOINT")
    }
    if (config.NodeID == "")
    {
        config.NodeID = os.Getenv("NODE_ID")
    }
    if (config.Endpoint == "" && config.NodeID == "")
    {
        fmt.Fprintf(os.Stderr, "Please set -endpoint and -node / CSI_ENDPOINT & NODE_ID env vars\n")
        os.Exit(1)
    }
    drv, err := vitastor.NewDriver(config)
    if (err != nil)
    {
        klog.Fatalln(err)
    }
    drv.Run()
}
