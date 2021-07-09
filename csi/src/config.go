// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

package vitastor

const (
    vitastorCSIDriverName    = "csi.vitastor.io"
    vitastorCSIDriverVersion = "0.6.5"
)

// Config struct fills the parameters of request or user input
type Config struct
{
    Endpoint string
    NodeID   string
}

// NewConfig returns config struct to initialize new driver
func NewConfig() *Config
{
    return &Config{}
}
