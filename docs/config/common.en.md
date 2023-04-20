[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Common Parameters

-----

[Читать на русском](common.ru.md)

# Common Parameters

These are the most common parameters which apply to all components of Vitastor.

- [config_path](#config_path)
- [etcd_address](#etcd_address)
- [etcd_prefix](#etcd_prefix)
- [log_level](#log_level)

## config_path

- Type: string
- Default: /etc/vitastor/vitastor.conf

Path to the JSON configuration file. Configuration file is optional,
a non-existing configuration file does not prevent Vitastor from
running if required parameters are specified.

## etcd_address

- Type: string or array of strings
- Can be changed online: yes

etcd connection endpoint(s). Multiple endpoints may be delimited by "," or
specified in a JSON array `["10.0.115.10:2379/v3","10.0.115.11:2379/v3"]`.
Note that https is not supported for etcd connections yet.

etcd connection endpoints can be changed online by updating global
configuration in etcd itself - this allows to switch the cluster to new
etcd addresses without downtime.

## etcd_prefix

- Type: string
- Default: /vitastor

Prefix for all keys in etcd used by Vitastor. You can change prefix and, for
example, use a single etcd cluster for multiple Vitastor clusters.

## log_level

- Type: integer
- Default: 0
- Can be changed online: yes

Log level. Raise if you want more verbose output.
