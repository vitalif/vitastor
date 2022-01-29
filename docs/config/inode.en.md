[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Image metadata in etcd

-----

[Читать на русском](inode.ru.md)

# Image metadata in etcd

Image list is stored in etcd in `/vitastor/config/inode/<pool>/<inode>` keys.

You can even create images manually:

```
etcdctl --endpoints=<etcd> put /vitastor/config/inode/<pool>/<inode> '{"name":"<name>","size":<size>[,"parent_id":<parent_inode_number>][,"readonly":true]}'
```

For example:

```
etcdctl --endpoints=http://10.115.0.10:2379/v3 put /vitastor/config/inode/1/1 '{"name":"testimg","size":2147483648}'
```

If you specify parent_id the image becomes a CoW clone. I.e. all writes go to the new inode and reads first check it
and then upper layers. You can then make parent readonly by updating its entry with `"readonly":true` for safety and
basically treat it as a snapshot.

So to create a snapshot you basically rename the previous upper layer (for example from testimg to testimg@0), make it readonly
and create a new top layer with the original name (testimg) and the previous one as a parent.

vitastor-cli, K8s, OpenStack and other drivers also store the reverse mapping in `/vitastor/index/image/<name>` keys
in JSON format: `{"id":<inode>,"pool_id":<pool>}` and ID counters in `/vitastor/index/maxid/<pool>` as numbers
to simplify ID generation.
