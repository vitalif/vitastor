# Security Parameters

These parameters affect your Vitastor installation security and apply to OSDs, monitors and clients.

Most of them can be set in /etc/vitastor/vitastor.conf and in etcd, but don't support online modification.

All certificate and private key parameters mentioned may contain a path to a PEM file or just
a PEM string with certificate or a private key. In the latter case, the string must begin with
"-----BEGIN CERTIFICATE-----" or "-----BEGIN PRIVATE KEY-----".
