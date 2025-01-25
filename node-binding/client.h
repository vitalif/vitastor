// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#ifndef NODE_VITASTOR_CLIENT_H
#define NODE_VITASTOR_CLIENT_H

#include <nan.h>
#include <vitastor_c.h>
#include <vitastor_kv.h>

class NodeVitastorRequest;

class NodeVitastor: public Nan::ObjectWrap
{
public:
    // constructor({ ...config })
    static NAN_METHOD(Create);
    // read(pool_id, inode, offset, len, callback(err, buffer, version))
    static NAN_METHOD(Read);
    // write(pool_id, inode, offset, buf: Buffer | Buffer[], { version }?, callback(err))
    static NAN_METHOD(Write);
    // sync(callback(err))
    static NAN_METHOD(Sync);
    // read_bitmap(pool_id, inode, offset, len, with_parents, callback(err, bitmap_buffer))
    static NAN_METHOD(ReadBitmap);
    // on_ready(callback(err))
    static NAN_METHOD(OnReady);
    // get_min_io_size(pool_id)
    static NAN_METHOD(GetMinIoSize);
    // get_max_atomic_write_size(pool_id)
    static NAN_METHOD(GetMaxAtomicWriteSize);
//    // destroy()
//    static NAN_METHOD(Destroy);

    ~NodeVitastor();

private:
    vitastor_c *c = NULL;
    int eventfd = -1;
    uv_poll_t poll_watcher;

    NodeVitastor();

    static void on_io_readable(uv_poll_t* handle, int status, int revents);
    static void on_read_finish(void *opaque, long retval, uint64_t version);
    static void on_ready_finish(void *opaque, long retval);
    static void on_write_finish(void *opaque, long retval);
    static void on_read_bitmap_finish(void *opaque, long retval, uint8_t *bitmap);

    NodeVitastorRequest* get_read_request(const Nan::FunctionCallbackInfo<v8::Value> & info, int argpos);
    NodeVitastorRequest* get_write_request(const Nan::FunctionCallbackInfo<v8::Value> & info, int argpos);

    friend class NodeVitastorImage;
    friend class NodeVitastorKV;
    friend class NodeVitastorKVListing;
};

class NodeVitastorImage: public Nan::ObjectWrap
{
public:
    // constructor(node_vitastor, name)
    static NAN_METHOD(Create);
    // read(offset, len, callback(err, buffer, version))
    static NAN_METHOD(Read);
    // write(offset, buf: Buffer | Buffer[], { version }?, callback(err))
    static NAN_METHOD(Write);
    // sync(callback(err))
    static NAN_METHOD(Sync);
    // read_bitmap(offset, len, with_parents, callback(err, bitmap_buffer))
    static NAN_METHOD(ReadBitmap);
    // get_info(callback({ num, name, size, parent_id?, readonly?, meta?, mod_revision, block_size, bitmap_granularity, immediate_commit }))
    static NAN_METHOD(GetInfo);

    ~NodeVitastorImage();

private:
    NodeVitastor *cli = NULL;
    std::string name;
    void *watch = NULL;
    std::vector<NodeVitastorRequest*> on_init;
    Nan::Persistent<v8::Object> cliObj;

    static void on_watch_start(void *opaque, long retval);
    void exec_request(NodeVitastorRequest *req);
    void exec_or_wait(NodeVitastorRequest *req);
};

class NodeVitastorKV: public Nan::ObjectWrap
{
public:
    // constructor(node_vitastor)
    static NAN_METHOD(Create);
    // open(pool_id, inode_num, { ...config }, callback(err))
    static NAN_METHOD(Open);
    // set_config({ ...config })
    static NAN_METHOD(SetConfig);
    // close(callback())
    static NAN_METHOD(Close);
    // get_size()
    static NAN_METHOD(GetSize);
    // get(key, callback(err, value))
    static NAN_METHOD(Get);
    // get_cached(key, callback(err, value))
    static NAN_METHOD(GetCached);
    // set(key, value, callback(err), cas_compare(old_value)?)
    static NAN_METHOD(Set);
    // del(key, callback(err), cas_compare(old_value)?)
    static NAN_METHOD(Del);
    // list(start_key?)
    static NAN_METHOD(List);

    ~NodeVitastorKV();

    static Nan::Persistent<v8::Function> listing_class;

private:
    NodeVitastor *cli = NULL;
    vitastorkv_dbw_t *dbw = NULL;

    static void get_impl(const Nan::FunctionCallbackInfo<v8::Value> & info, bool allow_cache);

    friend class NodeVitastorKVListing;
};

class NodeVitastorKVListing: public Nan::ObjectWrap
{
public:
    // constructor(node_vitastor_kv, start_key?)
    static NAN_METHOD(Create);
    // next(callback(err, value)?)
    static NAN_METHOD(Next);
    // close()
    static NAN_METHOD(Close);

    ~NodeVitastorKVListing();

private:
    NodeVitastorKV *kv = NULL;
    void *handle = NULL;
    NodeVitastorRequest *iter = NULL;
};

#endif
