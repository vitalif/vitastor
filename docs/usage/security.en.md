[Documentation](../../README.md#documentation) → Usage → Security in Vitastor

-----

[Читать на русском](security.ru.md)

# Security in Vitastor

- [Overview](#overview)
- [Quick setup](#quick-setup)
- Principles of operation
  - [etcd transport encryption (TLS)](#etcd-transport-encryption-tls)
  - [OSD transport encryption (AES-GCM)](#osd-transport-encryption-aes-gcm)
  - [End-to-end image data encryption (AES-XTS)](#end-to-end-image-data-encryption-aes-xts)
  - [Certificate-based authentication](#certificate-based-authentication)
  - [Users and access rights](#users-and-access-rights)
  - [etcd privileges](#etcd-privileges)
- Manual setup
  - [Configuring OSD transport encryption](#configuring-osd-transport-encryption)
  - etcd/Antietcd setup options
    - [Mon with embedded Antietcd](#mon-with-embedded-antietcd)
    - [Mon as an Etcd proxy](#mon-as-an-etcd-proxy)
    - [Mon with a separate Antietcd Proxy](#mon-with-a-separate-antietcd-proxy)
    - [Standalone Antietcd without etcd](#standalone-antietcd-without-etcd)
  - [Vault/OpenBao setup](#vaultopenbao-setup)
    - [Vault setup example](#vault-setup-example)
- Lists of allowed operations
  - [etcd data access rights](#etcd-data-access-rights)
  - [OSD data access rights](#osd-data-access-rights)
  - [API access rights](#api-access-rights)
- [Encryption performance](#encryption-performance)

## Overview

Starting from version 3.1.0, Vitastor provides full data protection:
control plane protection (etcd), data plane protection (OSDs), and end-to-end data encryption.

- Control plane protection:
  - etcd transport encryption (TLS)
  - Authentication via client TLS (X.509) certificates
  - Access control of clients to etcd data
- Data plane protection:
  - Full AES-GCM encryption of OSD transport (similar to TLS, but faster)
  - Alternatively, AES-GCM encryption of just operation headers with data checksums using a secret "salt"
  - Authentication via client TLS (X.509) certificates
  - Access control of clients on the OSD side
- End-to-end encryption:
  - Data is encrypted using AES-XTS on the client side, the Vitastor cluster has no access to plaintext data
  - AES-XTS keys can be stored in etcd or in an external Vault/OpenBao

All features are optional and disabled in the simplest configuration. By default, only
transport-level data checksums ([proto_checksums](../config/security.en.md#proto_checksums)=payload)
are enabled for clients that support them (>= 3.1.0). For older clients, connections
without data checksums are allowed by default ([force_proto_checksums](../config/security.en.md#force_proto_checksums) is empty).

For a quick setup, jump to the [Quick setup](#quick-setup) section.

Descriptions of all security-related parameters can be found [here](../config/security.en.md).

## Quick setup

For a quick setup, use the `/usr/lib/vitastor/mon/make-etcd` script:

1. Log in to the node where the first monitor and etcd will be located.
2. Create `/etc/vitastor/vitastor.conf` with minimal parameters: etcd_address,
   osd_network and, if you want to enable privileges, use_perms (note `https://`
   in etcd addresses):
   ```
   {
       "etcd_address": ["https://10.0.0.10:2379","https://10.0.0.11:2379","https://10.0.0.12:2379"],
       "osd_network": "10.0.0.0/24",
       "use_perms": true
   }
   ```
3. Run `/usr/lib/vitastor/mon/make-etcd` without parameters or with the `--antietcd-only`
   parameter if you want to initialize the cluster with Antietcd only, without etcd.
4. The script will generate all necessary certificates and offer to copy them to the other
   monitor nodes (agree!).
5. Log in to all other monitor nodes and repeat the `/usr/lib/vitastor/mon/make-etcd` call there.
6. If you also have nodes with OSDs only (without monitors), run the following command to
   copy only the required configuration to these nodes:
   ```
   /usr/lib/vitastor/mon/make-etcd --copy-to-osd osdnode1,osdnode2,...
   ```

After that, you can proceed with OSD initialization.

If you want to understand the setup in more detail, read the [Principles of operation](#principles-of-operation)
and [Manual setup](#manual-setup) sections below.

## Principles of operation

### etcd transport encryption (TLS)

Possible setups:
- Without encryption (http)
- With encryption (https)
- With encryption and client certificate authentication. Either the same certificate
  used for authentication on the OSD side (`cert`+`pkey` / `osd_cert`+`osd_pkey`)
  is used, or a separately specified certificate (`etcd_client_cert`+`etcd_client_key`).

### OSD transport encryption (AES-GCM)

Possible setups:
- Unencrypted transport without checksums: `proto_checksums=none`.
- Unencrypted transport with data checksums: `proto_checksums=payload` (may be omitted,
  this is the default value). It's allowed to disable checksums on the client side, or
  use an older client that does not support checksums. If you want to block connections
  from clients without checksums, use the option `force_proto_checksums=payload`.
- Header-only encryption with data checksums: activated when the options
  `cert`, `pkey`, `osd_ca` are set on the client side and `osd_cert`, `osd_pkey`, `osd_ca`, `client_ca`
  on the OSD side, with `proto_checksums=payload`. In this mode, disabling checksums on the client
  side is forbidden by default, i.e. `force_proto_checksums=payload` is used.
- Full transport encryption of all traffic: same as the previous option, but with `proto_checksums=gcm`.
  In this case, clients are by default allowed to downgrade to checksums only, but this
  can also be forbidden via `force_proto_checksums=gcm`. This is the slowest setup and
  it's only recommended for insecure (public) networks. In particular, full traffic
  encryption together with end-to-end AES-XTS image encryption encrypts data twice.

Encryption uses the AES-256-GCM algorithm and a custom simplified key exchange protocol,
fully analogous to TLS 1.3 ECDHE.

### End-to-end image data encryption (AES-XTS)

The Vitastor client supports encrypting each image's data with its own key. In this case,
data is encrypted by the client before sending it to OSDs and OSDs can't see it in plain.
The encryption key can be changed when cloning/creating image snapshots. For example,
you can make a base VM image (say, Debian Linux) unencrypted, but have encrypted client VM
images inheriting from it.

Image encryption keys can be stored in etcd or in an external Vault. In the latter case,
etcd only stores key IDs and Vitastor cluster can't decrypt the data at all. To use
Vault, create an image with the `--enc_key vault:ID` option, specify vault_url and vault_ca
options in the configuration, create accounts for all clients in Vault, and grant them access
to the required v1 secrets.

Once again, if AES-XTS is used together with full traffic encryption (`proto_checksums=gcm`),
image data is encrypted twice — first with AES-XTS, and then with AES-GCM. Use it only if
you are completely paranoid :-).

### Certificate-based authentication

When encryption is enabled, Vitastor clients, OSDs, and monitors authenticate via certificates
for both etcd (Antietcd) and OSD connections.

Separate certificates must be used for OSDs and monitors — either self-signed, or signed
by separate CAs (`osd_ca` and `mon_ca`). All OSDs can use the same certificate, and all
monitors can also use the same certificate, since the privileges of different OSDs or
different monitors do not differ (theoretically, one could differentiate OSD certificates
by pool, but there has been no need for this so far).

Also, a monitor certificate may not be needed at all if Antietcd is embedded into the monitor
itself. In this case, the monitor already has access to all etcd data directly in memory.

### Users and access rights

When transport encryption is disabled, Vitastor operates without access control, i.e.,
any cluster client has full access to both the management layer and the data layer. This
option is suitable for dedicated trusted storage networks.

When OSD transport encryption is enabled (at least for headers), you can enable access
control by turning on the `use_perms=true` option. When this option is enabled, each user
(and even an OSD or a monitor) can perform only operations allowed for him.

Each user (or administrator) must have their own certificate signed by a common root
certificate for clients (`client_ca`), with a Common Name equal to the user name.
Privilege settings are stored in etcd. OSDs and monitors don't need user accounts;
they authenticate via separate certificates.

User privileges are stored in etcd data under the keys `/vitastor/config/user/<name>`.
The following is defined per user in this key:
- Type:
  - Client (`type=client` or omitted) — can only read and modify explicitly permitted images.
  - Administrator (`type=admin`) — can read and modify all images, and also administer the
    cluster: view overall statistics and status, create and delete OSDs, etc.
- List of group names the user is a member of.

Images have the following properties:
- Owner (owner) — the user name that is allowed to both read and modify the image
- Owner group (owner_group) — the owner group name
- Reader group (reader_group) — the name of the group of users allowed to read the image

And there is also a property on the pool:
- Creator group (creator_group) — the name of the group of users allowed to create images in the pool

For the list of allowed operations on image data on the OSD side, see the
[OSD data access rights](#osd-data-access-rights) section.

### etcd privileges

etcd privileges are implemented through Antietcd in all modes of operation.

Built-in etcd privileges are not supported due to numerous inconveniences:
- Certificate-based authentication does not work at all in etcd's REST interface,
- Privileges are stored separately from k/v data and cannot participate in transactions,
- Only the administrator (root) can change privileges,
- There is no support for filtering range read responses by privileges.

If etcd is used, Antietcd acts as a filtering proxy and can be embedded in the Vitastor
monitor or run separately. In this case, etcd must allow incoming connections only from
Antietcd, and all other components must connect to Antietcd.

If Antietcd runs as a part of the Vitastor monitor, it is sufficient to enable the option
`use_perms=true` and set the required certificates. If Antietcd is run separately, privileges
have to be enabled separately using Antietcd options. For more details on the setup, see
the [etcd/Antietcd setup options](#etcdantietcd-setup-options) section.

For the list of allowed operations with etcd data, see the
[etcd data access rights](#etcd-data-access-rights) section.

## Manual setup

### Configuring OSD transport encryption

You need 2 certificates: one for OSDs and one for signing all client certificates.
For OSDs, you can use a self-signed certificate (osd_ca.crt) or a separate certificate (osd.crt)
signed by a trusted osd_ca.crt certificate. For clients, you must use separate certificates
signed by a common trusted (client_ca.crt).

Add to the Vitastor configuration on OSD servers:
- use_perms: true
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- osd_cert: osd_ca.crt
- osd_pkey: osd_ca.key

On the client side:
- use_perms: true
- cert: client.crt
- pkey: client.key

### etcd/Antietcd setup options

The following configuration options are available:

#### Mon with embedded Antietcd

The simplest option. You need 1 certificate for Antietcd (antietcd.crt), plus root
certificates for OSDs and clients.

Vitastor settings (`/etc/vitastor/vitastor.conf`):
- etcd_address: [ "http://mon1:2379", ... ] (addresses of your monitors with port 2379)
- use_perms: true
- use_antietcd: true
- antietcd_cert: antietcd.crt
- antietcd_key: antietcd.key
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt

#### Mon as an Etcd proxy

If you want to enable privileges, but stay on etcd, you can use etcd proxy mode.

You will need 2 separate certificates: one for etcd (etcd.crt) and one for antietcd (antietcd.crt).
The etcd client port must be different from the standard 2379 — for example, you can pick 2381.
OSD and client certificates are also needed.

Vitastor settings:
- etcd_address: [ "http://mon1:2379", ... ] (addresses of your monitors with port 2379)
- use_perms: true
- use_antietcd: true
- etcd_proxy:
  ```
  {
    "urls": [ "http://mon1:2381", ... ], // addresses of your etcd with port 2381
    "cert": "antietcd.crt",
    "key": "antietcd.key",
    "ca": "etcd.crt"
  }
  ```
- antietcd_cert: antietcd.crt
- antietcd_key: antietcd.key
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt

etcd command-line options:
```
--advertise-client-urls=https://<ADDRESS>:2381 --listen-client-urls=https://<ADDRESS>:2381 \
--client-cert-auth --cert-file=etcd.crt --key-file=etcd.key --trusted-ca-file=antietcd.crt \
--peer-client-cert-auth --peer-cert-file=etcd.crt --peer-key-file=etcd.key --peer-trusted-ca-file=etcd.crt
```

#### Mon with a separate Antietcd Proxy

If in addition to the previous option you want to offload Antietcd from the Vitastor monitor's
tasks, you can run it separately.

Similar to the previous option, 2 certificates are needed: one for etcd and one for antietcd,
plus separate certificates for clients, OSDs, and monitors will be needed.

Vitastor settings:
- etcd_address: [ "http://mon1:2379", ... ] (addresses of your monitors with port 2379)
- use_perms: true
- use_antietcd: false
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- mon_etcd_client_cert: mon_ca.crt
- mon_etcd_client_key: mon_ca.key

Antietcd command-line options:
```
--port 2379 \
--client_cert_auth 1 --auth_filter vitastor_auth_filter.js --etcd_proxy url1,url2,... \
--cert antietcd.crt --key antietcd.key --ca client_ca.crt --osd_ca osd_ca.crt --mon_ca mon_ca.crt \
--etcd_cert antietcd.crt --etcd_key antietcd.key --etcd_ca etcd.crt
```

etcd command-line options (same as in the previous option):
```
--advertise-client-urls=https://<ADDRESS>:2381 --listen-client-urls=https://<ADDRESS>:2381 \
--client-cert-auth --cert-file=etcd.crt --key-file=etcd.key --trusted-ca-file=antietcd.crt \
--peer-client-cert-auth --peer-cert-file=etcd.crt --peer-key-file=etcd.key --peer-trusted-ca-file=etcd.crt
```

#### Standalone Antietcd without etcd

Same as the previous option, but etcd and its certificate are not needed:

Vitastor settings (same as in the previous option):
- etcd_address: [ "http://mon1:2379", ... ] (addresses of your monitors with port 2379)
- use_perms: true
- use_antietcd: false
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- mon_etcd_client_cert: mon_ca.crt
- mon_etcd_client_key: mon_ca.key

Antietcd command-line options:
```
--port 2379 \
--client_cert_auth 1 --auth_filter vitastor_auth_filter.js \
--persist_filter vitastor_persist_filter.js \
--cert antietcd.crt --key antietcd.key --ca client_ca.crt --osd_ca osd_ca.crt --mon_ca mon_ca.crt
```

### Vault/OpenBao setup

To use Vault, each client that needs to get image keys from Vault needs a Vault account.
Vitastor only supports client certificate-based authentication, so all client certificates
(`cert`+`pkey`) must be registered in Vault, and they must be granted access to the
corresponding secrets (v1 secrets API is supported).

The required format of a Vault secret is a single `key` field as a hexadecimal string.
The AES-256-XTS algorithm is used, so the key length is 64 bytes, i.e., the string must
consist of 128 hexadecimal digits.

To connect to Vault, set the following settings in Vitastor.conf:
- `vault_url` — Vault address (e.g., `https://vault:8200`)
- `vault_ca` — Vault's own certificate

After that, if you create an image (`vitastor-cli create`) with the option `--enc_key vault:<ID>`,
Vitastor clients will first contact Vault to obtain a token at `/v1/auth/cert/login`,
and then request the actual secret from Vault at `/v1/secret/<ID>`.

#### Vault setup example

Step-by-step instructions for setting up a test Vault using OpenBao as an example:

1. If TLS is not yet configured, generate a self-signed TLS certificate for Vault:
   ```
   openssl req -days 3650 -x509 -addext basicConstraints=critical,CA:TRUE,pathlen:1 --addext subjectAltName=DNS:vault \
       -new -newkey rsa:4096 -nodes -keyout /etc/openbao/vault.key -out /etc/openbao/vault.crt
   ```
   Configure it in `/etc/openbao/openbao.hcl`:
   ```
   listener "tcp" {
       address       = "0.0.0.0:8200"
       tls_cert_file = "/etc/openbao/vault.crt"
       tls_key_file  = "/etc/openbao/vault.key"
   }
   ```
   And restart OpenBao (`systemctl restart openbao`).
2. Copy Vault's TLS certificate for Vitastor:
   ```
   cp /etc/openbao/vault.crt /etc/vitastor/vault.crt
   ```
   Transfer it to all client nodes and specify it in `/etc/vitastor/vitastor.conf`:
   ```
   {
       ...
       "vault_url": "http://vault:8200",
       "vault_ca": "/etc/vitastor/vault.crt"
   }
   ```
3. Check Vault status:
   ```
   bao status -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
4. Initialize Vault in test mode from 1 node (with 1 key share):
   ```
   bao operator init -n 1 -t 1 -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
5. Unseal Vault:
   ```
   bao operator unseal -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
6. Enable certificate-based authentication:
   ```
   bao auth enable -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 cert
   ```
7. Enable v1 secrets:
   ```
   bao secrets enable -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 -path=secret kv-v1
   ```
8. Create a test secret:
   ```
   bao kv put -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 secret/vitastor/testimg3 key=$(openssl rand -hex 64)
   ```
9. Generate a signed certificate for a Vitastor user (on a machine where you have `client_ca.crt` and `client_ca.key`):
   ```
   openssl req -subj '/CN=testimg3' -nodes -new -keyout testimg3.key -out testimg3.csr
   openssl x509 -req -days 3650 -CA client_ca.crt -CAkey client_ca.key -CAcreateserial -in testimg3.csr -out testimg3.crt
   rm testimg3.csr
   ```
10. Create a user in Vault and grant it access to the secret:
    ```
    cat >testimg3.policy <<EOF
    path "/secret/vitastor/testimg3" {
        capabilities = ["read"]
    }
    EOF

    bao policy write -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 testimg3 testimg3.policy

    bao write -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 auth/cert/certs/testimg3 \
        certificate=@testimg3.crt display_name=testimg3 token_ttl=24h token_policies=testimg3
    ```
11. Test access to the secret:
    ```
    curl --cacert /etc/vitastor/vault.crt --cert testimg3.crt --key testimg3.key \
        --json '{}' https://vault:8200/v1/auth/cert/login
    ```
    A token will be printed, substitute it into the following request:
    ```
    curl --cacert /etc/vitastor/vault.crt --cert testimg3.crt --key testimg3.key \
        -H 'X-Vault-Token: <RECEIVED TOKEN>' https://vault:8200/v1/secret/vitastor/testimg3
    ```
12. Create an image in Vitastor with the given secret (as an administrator or someone who
    has the right to create images in your pool):
    ```
    vitastor-cli create -s 100G --enc_key vault:vitastor/testimg3 --owner testimg3 testimg3
    ```
13. Test access to the image as user testimg3:
    ```
    vitastor-cli --cert testimg3.crt --pkey testimg3.key dd if=/dev/urandom oimg=testimg3 bs=1M count=100
    ```

## Lists of allowed operations

### etcd data access rights

Below, all key names are given without the common prefix `/vitastor`.

Allowed operations with keys in Antietcd for clients (`type=client`):
- Read-only:
  - Always allowed:
    - `/config/global`
    - `/config/node_placement`
    - `/config/pools`
    - `/pg/config`
    - `/osd/state/*`
    - `/pg/state/*`
    - `/index/maxid/*`
  - For images [readable by the user](#users-and-access-rights):
    - `/config/inode/*`
    - `/index/image/*`
    - `/inode/stats/*`
- Read and write:
  - For pools in which the user can create images:
    - `/index/maxid/*`
  - For images owned by the user:
    - `/config/inode/*`
    - `/index/image/*`

Allowed operations with keys in Antietcd for administrators (`type=admin`):
- Read:
  - `/stats`
  - `/mon/*`
  - `/pg/*`
  - `/pgstats/*`
  - `/inode/stats/*`
  - `/pool/stats/*`
- Read and write:
  - `/config/*`
  - `/osd/*`
  - `/index/*`
  - `/pg/history/*`

Allowed operations with keys in etcd for OSDs:
- Read:
  - `/pg/config`
  - `/config/*`
- Read and write:
  - `/osd/*`
  - `/pg/state/*`
  - `/pg/history/*`
  - `/pgstats/*`

Allowed operations with keys in etcd for monitors:
- Read:
  - `/config/*`
  - `/osd/*`
  - `/pgstats/*`
- Read and write:
  - `/pg/config`
  - `/stats`
  - `/history/last_clean_pgs`
  - `/mon/*`
  - `/pg/history/*`
  - `/inode/stats/*`
  - `/pool/stats/*`

### OSD data access rights

When the `use_perms` option and encryption are enabled, OSDs authenticate clients via
certificates and allow each client only what is allowed by the access control model.

Client operations:
- READ — allowed for images the user has read access to.
- WRITE, DELETE, SCRUB — allowed for images the user has write access to.
- SYNC — the operation is not tied to an image and is always allowed.
- DESCRIBE — the operation is allowed only for administrators (used by the commands
  `vitastor-cli describe` and `fix`).
- PING — the operation is always allowed.
- SHOW_CONFIG — the operation is always allowed, however, if the client presents
  itself as an OSD in it, then it is verified that it uses a certificate signed by `osd_ca`.
- SEC_LIST (listing) — allowed for other OSDs and administrators with any parameters,
  and for regular clients only allowed for requests limited to an image the user has
  read access to.

Cluster operations — allowed only for other OSDs:
- SEC_READ
- SEC_WRITE
- SEC_WRITE_STABLE
- SEC_SYNC
- SEC_STABILIZE
- SEC_ROLLBACK
- SEC_DELETE
- SEC_READ_BMP
- SEC_LOCK

### API access rights

[vitastor-cli serve](../usage/cli.en.md#serve) also supports client authentication
via certificates. Only certificates signed by `client_ca` are accepted. A separate
certificate `server_cert` with the key `server_pkey` is used as the server certificate.

For `vitastor-cli serve` to work correctly, it itself must use a certificate
(`cert`+`pkey`) of a user with administrator rights (`type=admin`) to access Vitastor.

Regular clients (users with `type=client`), when accessing the API, are only allowed API operations on images
available to them either for reading (for read-only operations) or for writing (for modification):

- image/list — for images the user can read.
- image/create — for pools in which the user is allowed to create images, or for
  creating snapshots of images owned by the user.
- image/delete, image/flatten, image/modify — for images owned by the user.

All other operations are allowed only for administrators (`type=admin`).

## Encryption performance

You may wonder — how fast is all this wonderful encryption?

The answer is — it depends heavily on the CPU. On modern processors (with AVX512 with VAES
support) it is very fast — AES encryption speed can reach 10-20 GB/s and above. This
primarily concerns the CPU of client machines, because end-to-end encryption is performed
entirely on the client, and even though transport encryption runs both on clients and OSDs,
each client has only one thread, while there are many OSDs on the server side.

On older processors, the speed is noticeably worse — for example, on a Xeon E5 v4 it is
only 3 GB/s.

You can evaluate the performance of your processors using the `vitastor-cli cpubench` command.

Example output (💪 AMD EPYC 9575F):

```
$ vitastor-cli cpubench
Vitastor transport encryption benchmark (AES-256-GCM, AES-256-XTS and xxhash3)

Warmup...

No transport encryption, data checksums enabled, e2e unencrypted image
xxhash3 1 M block... 209000 iterations in 2001 ms = 104447.78 MB/s
xxhash3 4 K block... 37000000 iterations in 2022 ms = 71479.35 MB/s

Header encryption with payload checksums, e2e unencrypted image
AES-256-GCM encrypt header + xxhash3 1 M block... 210000 iterations in 2015 ms = 104218.36 MB/s
AES-256-GCM encrypt header + xxhash3 4 K block... 26000000 iterations in 2073 ms = 48993.01 MB/s

Full transport encryption, e2e unencrypted image
AES-256-GCM encrypt header and 1 M block... 54000 iterations in 2000 ms = 27000.00 MB/s
AES-256-GCM encrypt header and 4 K block... 11700000 iterations in 2014 ms = 22692.71 MB/s

No transport encryption, no checksums, e2e encrypted image
AES-256-XTS encrypt 1 M block... 50000 iterations in 2039 ms = 24521.82 MB/s
AES-256-XTS encrypt 4 K block... 12600000 iterations in 2009 ms = 24499.13 MB/s

No transport encryption, e2e encrypted image, data checksums enabled
AES-256-XTS encrypt + xxhash3 1 M block... 40000 iterations in 2013 ms = 19870.84 MB/s
AES-256-XTS encrypt + xxhash3 4 K block... 10200000 iterations in 2011 ms = 19812.90 MB/s

Header encryption with payload checksums, e2e encrypted image
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 1 M block... 40000 iterations in 2014 ms = 19860.97 MB/s
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 4 K block... 8700000 iterations in 2011 ms = 16899.24 MB/s

Full transport encryption, e2e encrypted image
AES-256-XTS + AES-256-GCM encrypt 1 M block... 26000 iterations in 2062 ms = 12609.12 MB/s
AES-256-XTS + AES-256-GCM encrypt 4 K block... 6300000 iterations in 2006 ms = 12267.88 MB/s
```

And here is Xeon E5-2680v4:

```
$ vitastor-cli cpubench
Vitastor transport encryption benchmark (AES-256-GCM, AES-256-XTS and xxhash3)

Warmup...

No transport encryption, data checksums enabled, e2e unencrypted image
xxhash3 1 M block... 62000 iterations in 2021 ms = 30677.88 MB/s
xxhash3 4 K block... 12400000 iterations in 2006 ms = 24146.31 MB/s

Header encryption with payload checksums, e2e unencrypted image
AES-256-GCM encrypt header + xxhash3 1 M block... 62000 iterations in 2027 ms = 30587.07 MB/s
AES-256-GCM encrypt header + xxhash3 4 K block... 6800000 iterations in 2011 ms = 13208.60 MB/s

Full transport encryption, e2e unencrypted image
AES-256-GCM encrypt header and 1 M block... 7000 iterations in 2317 ms = 3021.15 MB/s
AES-256-GCM encrypt header and 4 K block... 1500000 iterations in 2102 ms = 2787.52 MB/s

No transport encryption, no checksums, e2e encrypted image
AES-256-XTS encrypt 1 M block... 7000 iterations in 2317 ms = 3021.15 MB/s
AES-256-XTS encrypt 4 K block... 1600000 iterations in 2088 ms = 2993.30 MB/s

No transport encryption, e2e encrypted image, data checksums enabled
AES-256-XTS encrypt + xxhash3 1 M block... 6000 iterations in 2188 ms = 2742.23 MB/s
AES-256-XTS encrypt + xxhash3 4 K block... 1400000 iterations in 2053 ms = 2663.78 MB/s

Header encryption with payload checksums, e2e encrypted image
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 1 M block... 6000 iterations in 2190 ms = 2739.73 MB/s
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 4 K block... 1300000 iterations in 2101 ms = 2417.00 MB/s

Full transport encryption, e2e encrypted image
AES-256-XTS + AES-256-GCM encrypt 1 M block... 4000 iterations in 2666 ms = 1500.38 MB/s
AES-256-XTS + AES-256-GCM encrypt 4 K block... 800000 iterations in 2113 ms = 1478.94 MB/s
```
