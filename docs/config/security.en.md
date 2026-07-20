[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Security Parameters

-----

[Читать на русском](security.ru.md)

# Security Parameters

These parameters affect your Vitastor installation security and apply to OSDs, monitors and clients.

Most of them can be set in /etc/vitastor/vitastor.conf and in etcd, but don't support online modification.

All certificate and private key parameters mentioned may contain a path to a PEM file or just
a PEM string with certificate or a private key. In the latter case, the string must begin with
"-----BEGIN CERTIFICATE-----" or "-----BEGIN PRIVATE KEY-----".

- [use_perms](#use_perms)
- [cert](#cert)
- [pkey](#pkey)
- [etcd_ca](#etcd_ca)
- [client_ca](#client_ca)
- [admin_ca](#admin_ca)
- [osd_ca](#osd_ca)
- [mon_ca](#mon_ca)
- [antietcd_cert](#antietcd_cert)
- [antietcd_key](#antietcd_key)
- [etcd_proxy.urls](#etcd_proxyurls)
- [etcd_proxy.cert](#etcd_proxycert)
- [etcd_proxy.key](#etcd_proxykey)
- [etcd_proxy.ca](#etcd_proxyca)
- [osd_cert](#osd_cert)
- [osd_pkey](#osd_pkey)
- [api_cert](#api_cert)
- [api_pkey](#api_pkey)
- [etcd_client_cert](#etcd_client_cert)
- [etcd_client_key](#etcd_client_key)
- [osd_etcd_client_cert](#osd_etcd_client_cert)
- [osd_etcd_client_key](#osd_etcd_client_key)
- [mon_etcd_client_cert](#mon_etcd_client_cert)
- [mon_etcd_client_key](#mon_etcd_client_key)
- [proto_checksums](#proto_checksums)
- [force_proto_checksums](#force_proto_checksums)
- [max_cipher_pool_size](#max_cipher_pool_size)
- [vault_url](#vault_url)
- [vault_secret_api_path](#vault_secret_api_path)
- [vault_client_cert](#vault_client_cert)
- [vault_client_key](#vault_client_key)
- [vault_ca](#vault_ca)
- [vault_timeout_ms](#vault_timeout_ms)
- [vault_error_timeout_sec](#vault_error_timeout_sec)
- [vault_refresh_leeway_sec](#vault_refresh_leeway_sec)

## use_perms

- Type: boolean
- Default: false

Enable client permissions in a Vitastor cluster, including Antietcd built into the Monitor.
Requires configured encryption. Also note that separate Antietcd requires separate configuration
to use permissions (see [security documentation](../usage/security.en.md) for details).

## cert

- Type: string

Client certificate of the current Vitastor user. Required for Vitastor protocol encryption.
Must be signed with [client_ca](#client_ca). Also used as the client certificate for etcd/Antietcd
connections by default.

## pkey

- Type: string

Private key of the current Vitastor user.

## etcd_ca

- Type: string

Trusted TLS CA to verify etcd server certificate. Or just the etcd server's
certificate itself - it's fine to use it for etcd_ca.

## client_ca

- Type: string

Trusted TLS CA to verify Vitastor client certificates.
Mandatory for Vitastor protocol encryption.

## admin_ca

- Type: string

Trusted TLS CA to verify Vitastor administrator certificates.

## osd_ca

- Type: string

Trusted TLS CA to verify Vitastor OSD certificates. Also mandatory for Vitastor protocol
encryption. Must be different from client_ca. May be equal to osd_cert - different OSDs
don't require separate certificates at the moment because their permissions don't differ.

## mon_ca

- Type: string

Trusted TLS CA to verify Vitastor Monitor certificates. Used only for separate Antietcd,
not required when a monitor built-in Antietcd is used. May be equal to mon_client_etcd_cert.

## antietcd_cert

- Type: string

Server TLS certificate for Antietcd built into the Monitor.

## antietcd_key

- Type: string

Private key for antietcd_cert.

## etcd_proxy.urls

- Type: string or array of strings

etcd URLs for Antietcd etcd proxy mode.
See [Mon as Etcd proxy](../usage/security.en.md#mon-as-etcd-proxy) for details.

## etcd_proxy.cert

- Type: string

Client certificate for Antietcd connections to etcd in proxy mode.

## etcd_proxy.key

- Type: string

Private key for etcd_proxy.cert.

## etcd_proxy.ca

- Type: string

Trusted TLS CA to verify etcd server certificate when connecting to it from Antietcd.

## osd_cert

- Type: string

Vitastor OSD server certificate. Required for Vitastor protocol encryption. May be equal
to [osd_ca](#osd_ca) - all OSDs share the same permission set for now. Also used as the client
certificate for connections from OSD to etcd/Antietcd by default.

## osd_pkey

- Type: string

Private key for osd_cert.

## api_cert

- Type: string

Server TLS certificate for [vitastor-cli serve](../usage/cli.en.md#serve) API server.

## api_pkey

- Type: string

Private key for api_cert.

## etcd_client_cert

- Type: string

Client TLS certificate to use for connections from Vitastor clients to etcd/Antietcd if you don't want
to use the common client certificate [cert](#cert).

## etcd_client_key

- Type: string

Private key for etcd_client_cert.

## osd_etcd_client_cert

- Type: string

Client TLS certificate to use for connections from Vitastor OSDs to etcd/Antietcd if you don't want
to use the common OSD certificate [osd_cert](#osd_cert).

## osd_etcd_client_key

- Type: string

Private key for osd_etcd_client_cert.

## mon_etcd_client_cert

- Type: string

Client TLS certificate to use for connections from Vitastor Monitors to etcd/Antietcd - required
if you don't use the built-in Antietcd. In case you use it Monitor has direct access to Antietcd data
and doesn't require any connection.

## mon_etcd_client_key

- Type: string

Private key for mon_etcd_client_cert.

## proto_checksums

- Type: string
- Default: payload

One of "full", "payload", "gcm", "none":
- "full" means calculate and verify transport level checksums from the full message data
  including the header - recommended for unencrypted setups.
- "payload" enables checksums only for the actual read/write data, but skips them for message
  headers - recommended for encrypted setups because headers are already protected by AES-GCM.
- "gcm" disables checksums and enables AES-GCM encryption of the whole messages including headers
  and data - AES-GCM already includes MAC which is actually a stronger checksum. This option is
  slower and is only recommended for untrusted networks.
- "none" disables transport level checksums at all.

## force_proto_checksums

- Type: string

To allow older clients to connect to a Vitastor cluster with enabled checksums, Vitastor OSDs
allow clients to downgrade their proto_checksums by default. force_proto_checksums sets the
minimum security level allowed for connecting clients. When encryption is disabled, default
force_proto_checksums is none and clients without checksums are allowed. With enabled
encryption, force_proto_checksums becomes "payload" by default to block unauthenticated data
on the transport level.

## max_cipher_pool_size

- Type: integer
- Default: 256

Maximum number of OpenSSL cipher contexts cached in OSD memory, counted separately
for each cipher and for encryption/decryption. Probably doesn't require modification.

## vault_url

- Type: string

Vault base URL.

Vitastor clients support AES-256-XTS image data encryption with different per-image keys.
Encryption is performed by the client, OSDs don't have access to decrypted data.

Encryption keys may be stored in etcd or, for the increased security level, in an external
[HashiCorp Vault](https://developer.hashicorp.com/vault/) or [OpenBao](https://openbao.org/)
instance.

Vitastor clients use [v1 k/v secrets engine](https://openbao.org/api-docs/secret/kv/kv-v1/)
and [TLS authentication engine](https://openbao.org/api-docs/auth/cert/) in Vault.

In that case, only key IDs are stored in etcd.

## vault_secret_api_path

- Type: string
- Default: /v1/secret/

Vault v1 secret API mount path to use.

## vault_client_cert

- Type: string

Client TLS certificate to use for Vault connections if you don't want to use the common Vitastor
client certificate [cert](#cert) which is also used for Vault connections by default.

## vault_client_key

- Type: string

Private key for the vault_client_cert certificate.

## vault_ca

- Type: string

Trusted TLS CA to verify Vault server certificate. May be path to a file,
directory or just a PEM string with certificate.

## vault_timeout_ms

- Type: integer
- Default: 5000

Timeout for Vault requests in milliseconds.

## vault_error_timeout_sec

- Type: integer
- Default: 60

Time (in seconds) to wait before retrying after receiving an error from Vault.

## vault_refresh_leeway_sec

- Type: integer
- Default: 60

Extra time (in seconds) before real Vault token lease_timeout to refresh it, just
in case of system clock drift.
