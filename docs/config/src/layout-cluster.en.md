# Cluster-Wide Disk Layout Parameters

These parameters apply to clients and OSDs, are fixed at the moment of OSD drive
initialization and can't be changed after it without losing data.

OSDs with different values of these parameters (for example, SSD and SSD+HDD
OSDs) can coexist in one Vitastor cluster within different pools. Each pool can
only include OSDs with identical settings of these parameters.

These parameters, when set to a non-default value, must also be specified in
etcd for clients to be aware of their values, either in /vitastor/config/global
or in pool configuration. Pool configuration overrides the global setting.
If the value for a pool in etcd doesn't match on-disk OSD configuration, the
OSD will refuse to start PGs of that pool.
