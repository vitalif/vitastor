[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Monitor Parameters

-----

[Читать на русском](monitor.ru.md)

# Monitor Parameters

These parameters only apply to Monitors.

- [use_antietcd](#use_antietcd)
- [enable_prometheus](#enable_prometheus)
- [mon_http_port](#mon_http_port)
- [mon_http_ip](#mon_http_ip)
- [mon_https_cert](#mon_https_cert)
- [mon_https_key](#mon_https_key)
- [mon_https_client_auth](#mon_https_client_auth)
- [mon_https_ca](#mon_https_ca)
- [etcd_mon_ttl](#etcd_mon_ttl)
- [etcd_mon_timeout](#etcd_mon_timeout)
- [etcd_mon_retries](#etcd_mon_retries)
- [mon_change_timeout](#mon_change_timeout)
- [mon_stats_timeout](#mon_stats_timeout)
- [osd_out_time](#osd_out_time)
- [placement_levels](#placement_levels)
- [use_old_pg_combinator](#use_old_pg_combinator)
- [osd_backfillfull_ratio](#osd_backfillfull_ratio)

## use_antietcd

- Type: boolean
- Default: false

Enable experimental built-in etcd replacement (clustered key-value database):
[antietcd](https://git.yourcmc.ru/vitalif/antietcd/).

When set to true, monitor runs internal antietcd automatically if it finds
a network interface with an IP address matching one of addresses in the
`etcd_address` configuration option (in `/etc/vitastor/vitastor.conf` or in
the monitor command line). If there are multiple matching addresses, it also
checks `antietcd_port` and antietcd is started for address with matching port.
By default, antietcd accepts connection on the selected IP address, but it
can also be overridden manually in the `antietcd_ip` option.

When antietcd is started, monitor stores cluster metadata itself and exposes
a etcd-compatible REST API. On disk, these metadata are stored in
`/var/lib/vitastor/mon_2379.json.gz` (can be overridden in antietcd_data_file
or antietcd_data_dir options). All other antietcd parameters
(see [here](https://git.yourcmc.ru/vitalif/antietcd/)) except node_id,
cluster, cluster_key, persist_filter, stale_read can also be set in
Vitastor configuration with `antietcd_` prefix.

You can dump/load data to or from antietcd using Antietcd `anticli` tool:

```
npm exec anticli -e http://etcd:2379/v3 get --prefix '' --no-temp > dump.json
npm exec anticli -e http://antietcd:2379/v3 load < dump.json
```

## enable_prometheus

- Type: boolean
- Default: true

Enable built-in Prometheus metrics exporter at mon_http_port (8060 by default).

Note that only the active (master) monitor exposes metrics, others return
HTTP 503. So you should add all monitor URLs to your Prometheus job configuration.

Grafana dashboard suitable for this exporter is here: [Vitastor-Grafana-6+.json](../../mon/scripts/Vitastor-Grafana-6+.json).

## mon_http_port

- Type: integer
- Default: 8060

HTTP port for monitors to listen on (including metrics exporter)

## mon_http_ip

- Type: string

IP address for monitors to listen on (all addresses by default)

## mon_https_cert

- Type: string

Path to PEM SSL certificate file for monitor to listen using HTTPS

## mon_https_key

- Type: string

Path to PEM SSL private key file for monitor to listen using HTTPS

## mon_https_client_auth

- Type: boolean
- Default: false

Enable HTTPS client certificate-based authorization for monitor connections

## mon_https_ca

- Type: string

Path to CA certificate for client HTTPS authorization

## etcd_mon_ttl

- Type: seconds
- Default: 1
- Minimum: 5

Monitor etcd lease refresh interval in seconds

## etcd_mon_timeout

- Type: milliseconds
- Default: 1000

etcd request timeout used by monitor

## etcd_mon_retries

- Type: integer
- Default: 5

Maximum number of attempts for one monitor etcd request

## mon_change_timeout

- Type: milliseconds
- Default: 1000
- Minimum: 100

Optimistic retry interval for monitor etcd modification requests

## mon_stats_timeout

- Type: milliseconds
- Default: 1000
- Minimum: 100

Interval for monitor to wait before updating aggregated statistics in
etcd after receiving OSD statistics updates

## osd_out_time

- Type: seconds
- Default: 600

Time after which a failed OSD is removed from the data distribution.
I.e. time which the monitor waits before attempting to restore data
redundancy using other OSDs.

## placement_levels

- Type: json
- Default: `{"host":100,"osd":101}`

Levels for the placement tree. You can define arbitrary tree levels by
defining them in this parameter. The configuration parameter value should
contain a JSON object with level names as keys and integer priorities as
values.  Smaller priority means higher level in tree. For example,
"datacenter" should have smaller priority than "osd". "host" and "osd"
levels are always predefined and can't be removed. If one of them is not
present in the configuration, then it is defined with the default priority
(100 for "host", 101 for "osd").

## use_old_pg_combinator

- Type: boolean
- Default: false

Use the old PG combination generator which doesn't support [level_placement](pool.en.md#level_placement)
and [raw_placement](pool.en.md#raw_placement) for pools which don't use this features.

## osd_backfillfull_ratio

- Type: number
- Default: 0.99

Monitors try to prevent OSDs becoming 100% full during rebalance or recovery by
calculating how much space will be occupied on every OSD after all rebalance
and recovery operations finish, and pausing rebalance and recovery if that
amount of space exceeds OSD capacity multiplied by the value of this
configuration parameter.

Future used space is calculated by summing space used by all user data blocks
(objects) in all PGs placed on a specific OSD, even if some of these objects
currently reside on a different set of OSDs.
