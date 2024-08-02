// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "addon.h"

#define NODE_VITASTOR_READ 1
#define NODE_VITASTOR_WRITE 2
#define NODE_VITASTOR_SYNC 3
#define NODE_VITASTOR_READ_BITMAP 4
#define NODE_VITASTOR_GET_INFO 5

#ifndef INODE_POOL
#define INODE_POOL(inode) (uint32_t)((inode) >> (64 - POOL_ID_BITS))
#define INODE_NO_POOL(inode) (uint64_t)((inode) & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1))
#define INODE_WITH_POOL(pool_id, inode) (((uint64_t)(pool_id) << (64-POOL_ID_BITS)) | INODE_NO_POOL(inode))
#endif

class NodeVitastorRequest: public Nan::AsyncResource
{
public:
    NodeVitastorRequest(NodeVitastor *cli, v8::Local<v8::Function> cb): Nan::AsyncResource("NodeVitastorRequest")
    {
        this->cli = cli;
        callback.Reset(cb);
    }

    iovec iov;
    std::vector<iovec> iov_list;
    NodeVitastor *cli = NULL;
    NodeVitastorImage *img = NULL;
    int op = 0;
    uint64_t offset = 0, len = 0, version = 0;
    bool with_parents = false;
    Nan::Persistent<v8::Function> callback;
};

static uint64_t get_ui64(const v8::Local<v8::Value> & val)
{
    if (val->IsBigInt())
        return val->ToBigInt(Nan::GetCurrentContext()).ToLocalChecked()->Uint64Value();
    return Nan::To<int64_t>(val).FromJust();
}

//////////////////////////////////////////////////
// NodeVitastor
//////////////////////////////////////////////////

NodeVitastor::NodeVitastor(): Nan::ObjectWrap()
{
    TRACE("NodeVitastor: constructor");
    poll_watcher.data = this;
}

NodeVitastor::~NodeVitastor()
{
    TRACE("NodeVitastor: destructor");
    uv_poll_stop(&poll_watcher);
    vitastor_c_destroy(c);
    c = NULL;
}

NAN_METHOD(NodeVitastor::Create)
{
    TRACE("NodeVitastor::Create");
    v8::Local<v8::Object> jsParams = info[0].As<v8::Object>();
    v8::Local<v8::Array> keys = Nan::GetOwnPropertyNames(jsParams).ToLocalChecked();
    std::vector<std::string> cfg;
    for (uint32_t i = 0; i < keys->Length(); i++)
    {
        auto key = Nan::Get(keys, i).ToLocalChecked();
        cfg.push_back(std::string(*Nan::Utf8String(key)));
        cfg.push_back(std::string(*Nan::Utf8String(Nan::Get(jsParams, key).ToLocalChecked())));
    }

    const char **c_cfg = new const char*[cfg.size()];
    for (size_t i = 0; i < cfg.size(); i++)
    {
        c_cfg[i] = cfg[i].c_str();
    }
    NodeVitastor* cli = new NodeVitastor();
    cli->c = vitastor_c_create_uring_json(c_cfg, cfg.size());
    delete[] c_cfg;

    int res = vitastor_c_uring_register_eventfd(cli->c);
    if (res >= 0)
    {
        cli->eventfd = res;
        res = uv_poll_init(uv_default_loop(), &cli->poll_watcher, cli->eventfd);
        if (res >= 0)
            res = uv_poll_start(&cli->poll_watcher, UV_READABLE, on_io_readable);
        on_io_readable(&cli->poll_watcher, 0, UV_READABLE);
    }
    if (res < 0)
    {
        ERRORF("NodeVitastor: failed to create and register io_uring eventfd in libuv: %s", strerror(-cli->eventfd));
        vitastor_c_destroy(cli->c);
        cli->c = NULL;
        Nan::ThrowError("failed to create and register io_uring eventfd");
        return;
    }

    cli->Wrap(info.This());
    info.GetReturnValue().Set(info.This());
}

void NodeVitastor::on_io_readable(uv_poll_t* handle, int status, int revents)
{
    TRACEF("NodeVitastor::on_io_readable status/revents %d %d", status, revents);
    if (revents & UV_READABLE)
    {
        NodeVitastor* self = (NodeVitastor*)handle->data;
        vitastor_c_uring_handle_events(self->c);
    }
}

NodeVitastorRequest* NodeVitastor::get_read_request(const Nan::FunctionCallbackInfo<v8::Value> & info, int argpos)
{
    uint64_t offset = get_ui64(info[argpos+0]);
    uint64_t len = get_ui64(info[argpos+1]);
    uint8_t *buf = (uint8_t*)malloc(len);
    if (!buf)
    {
        Nan::ThrowError("failed to allocate memory");
        return NULL;
    }
    v8::Local<v8::Function> callback = info[argpos+2].As<v8::Function>();
    auto req = new NodeVitastorRequest(this, callback);

    req->offset = offset;
    req->len = len;
    req->iov = { .iov_base = buf, .iov_len = len };

    return req;
}

// read(pool, inode, offset, len, callback(err, buffer, version))
NAN_METHOD(NodeVitastor::Read)
{
    TRACE("NodeVitastor::Read");
    if (info.Length() < 5)
        Nan::ThrowError("Not enough arguments to read(pool, inode, offset, len, callback(err, buffer, version))");

    NodeVitastor* self = Nan::ObjectWrap::Unwrap<NodeVitastor>(info.This());

    uint64_t pool = get_ui64(info[0]);
    uint64_t inode = get_ui64(info[1]);

    auto req = self->get_read_request(info, 2);

    self->Ref();
    vitastor_c_read(self->c, ((pool << (64-POOL_ID_BITS)) | inode), req->offset, req->len, &req->iov, 1, on_read_finish, req);
}

NodeVitastorRequest* NodeVitastor::get_write_request(const Nan::FunctionCallbackInfo<v8::Value> & info, int argpos)
{
    uint64_t offset = get_ui64(info[argpos+0]);
    const auto & bufarg = info[argpos+1];
    uint64_t version = 0;
    if (!info[argpos+2].IsEmpty() &&
        !info[argpos+2]->IsFunction() &&
        info[argpos+2]->IsObject())
    {
        auto key = Nan::New<v8::String>("version").ToLocalChecked();
        auto params = info[argpos+2].As<v8::Object>();
        auto versionObj = Nan::Get(params, key).ToLocalChecked();
        if (!versionObj.IsEmpty())
            version = get_ui64(versionObj);
        argpos++;
    }

    v8::Local<v8::Function> callback = info[argpos+2].As<v8::Function>();
    auto req = new NodeVitastorRequest(this, callback);

    req->offset = offset;
    req->version = version;

    if (bufarg->IsArray())
    {
        auto buffers = bufarg.As<v8::Array>();
        req->len = 0;
        for (uint32_t i = 0; i < buffers->Length(); i++)
        {
            auto buffer_obj = Nan::Get(buffers, i).ToLocalChecked();
            char *buf = node::Buffer::Data(buffer_obj);
            uint64_t len = node::Buffer::Length(buffer_obj);
            req->iov_list.push_back({ .iov_base = buf, .iov_len = len });
            req->len += len;
        }
    }
    else
    {
        char *buf = node::Buffer::Data(bufarg);
        uint64_t len = node::Buffer::Length(bufarg);
        req->iov = { .iov_base = buf, .iov_len = len };
        req->len = len;
    }

    return req;
}

// write(pool, inode, offset, buf: Buffer | Buffer[], { version }?, callback(err))
NAN_METHOD(NodeVitastor::Write)
{
    TRACE("NodeVitastor::Write");
    if (info.Length() < 5)
        Nan::ThrowError("Not enough arguments to write(pool, inode, offset, buf: Buffer | Buffer[], { version }?, callback(err))");

    NodeVitastor* self = Nan::ObjectWrap::Unwrap<NodeVitastor>(info.This());

    uint64_t pool = get_ui64(info[0]);
    uint64_t inode = get_ui64(info[1]);

    auto req = self->get_write_request(info, 2);

    self->Ref();
    vitastor_c_write(self->c, ((pool << (64-POOL_ID_BITS)) | inode), req->offset, req->len, req->version,
        req->iov_list.size() ? req->iov_list.data() : &req->iov,
        req->iov_list.size() ? req->iov_list.size() : 1,
        on_write_finish, req);
}

// sync(callback(err))
NAN_METHOD(NodeVitastor::Sync)
{
    TRACE("NodeVitastor::Sync");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to sync(callback(err))");

    NodeVitastor* self = Nan::ObjectWrap::Unwrap<NodeVitastor>(info.This());

    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    auto req = new NodeVitastorRequest(self, callback);

    self->Ref();
    vitastor_c_sync(self->c, on_write_finish, req);
}

// read_bitmap(pool, inode, offset, len, with_parents, callback(err, bitmap_buffer))
NAN_METHOD(NodeVitastor::ReadBitmap)
{
    TRACE("NodeVitastor::ReadBitmap");
    if (info.Length() < 6)
        Nan::ThrowError("Not enough arguments to read_bitmap(pool, inode, offset, len, with_parents, callback(err, bitmap_buffer))");

    NodeVitastor* self = Nan::ObjectWrap::Unwrap<NodeVitastor>(info.This());

    uint64_t pool = get_ui64(info[0]);
    uint64_t inode = get_ui64(info[1]);
    uint64_t offset = get_ui64(info[2]);
    uint64_t len = get_ui64(info[3]);
    bool with_parents = Nan::To<bool>(info[4]).FromJust();
    v8::Local<v8::Function> callback = info[5].As<v8::Function>();

    auto req = new NodeVitastorRequest(self, callback);
    self->Ref();
    vitastor_c_read_bitmap(self->c, ((pool << (64-POOL_ID_BITS)) | inode), offset, len, with_parents, on_read_bitmap_finish, req);
}

static void on_error(NodeVitastorRequest *req, Nan::Callback & nanCallback, long retval)
{
    // Legal errors: EINVAL, EIO, EROFS, ENOSPC, EINTR, ENOENT
    v8::Local<v8::Value> args[1];
    if (!retval)
        args[0] = Nan::Null();
    else
        args[0] = Nan::New<v8::Int32>((int32_t)retval);
    nanCallback.Call(1, args, req);
}

void NodeVitastor::on_read_finish(void *opaque, long retval, uint64_t version)
{
    TRACE("NodeVitastor::on_read_finish");
    Nan::HandleScope scope;
    NodeVitastorRequest *req = (NodeVitastorRequest *)opaque;
    Nan::Callback nanCallback(Nan::New(req->callback));
    if (retval == -ENOENT)
    {
        free(req->iov.iov_base);
        nanCallback.Call(0, NULL, req);
    }
    else if (retval < 0 || (uint64_t)retval != req->len)
    {
        free(req->iov.iov_base);
        on_error(req, nanCallback, retval);
    }
    else
    {
        v8::Local<v8::Value> args[3];
        args[0] = Nan::Null();
        args[1] = Nan::NewBuffer((char*)req->iov.iov_base, req->iov.iov_len).ToLocalChecked();
        args[2] = v8::BigInt::NewFromUnsigned(v8::Isolate::GetCurrent(), version);
        nanCallback.Call(3, args, req);
    }
    req->cli->Unref();
    delete req;
}

void NodeVitastor::on_write_finish(void *opaque, long retval)
{
    TRACE("NodeVitastor::on_write_finish");
    Nan::HandleScope scope;
    NodeVitastorRequest *req = (NodeVitastorRequest *)opaque;
    Nan::Callback nanCallback(Nan::New(req->callback));
    if (retval < 0 || (uint64_t)retval != req->len)
    {
        on_error(req, nanCallback, retval);
    }
    else
    {
        v8::Local<v8::Value> args[1];
        args[0] = Nan::Null();
        nanCallback.Call(1, args, req);
    }
    req->cli->Unref();
    delete req;
}

void NodeVitastor::on_read_bitmap_finish(void *opaque, long retval, uint8_t *bitmap)
{
    TRACE("NodeVitastor::on_read_bitmap_finish");
    Nan::HandleScope scope;
    NodeVitastorRequest *req = (NodeVitastorRequest *)opaque;
    Nan::Callback nanCallback(Nan::New(req->callback));
    if (retval == -ENOENT)
        nanCallback.Call(0, NULL, req);
    else if (retval < 0)
        on_error(req, nanCallback, retval);
    else
    {
        v8::Local<v8::Value> args[2];
        args[0] = Nan::Null();
        args[1] = Nan::NewBuffer((char*)bitmap, (retval+7)/8).ToLocalChecked();
        nanCallback.Call(2, args, req);
    }
    req->cli->Unref();
    delete req;
}

//NAN_METHOD(NodeVitastor::Destroy)
//{
//    TRACE("NodeVitastor::Destroy");
//}

//////////////////////////////////////////////////
// NodeVitastorImage
//////////////////////////////////////////////////

NAN_METHOD(NodeVitastorImage::Create)
{
    TRACE("NodeVitastorImage::Create");
    if (info.Length() < 2)
        Nan::ThrowError("Not enough arguments to Image(client, name)");

    v8::Local<v8::Object> parent = info[0].As<v8::Object>();
    std::string name = std::string(*Nan::Utf8String(info[1].As<v8::String>()));
    NodeVitastor *cli = Nan::ObjectWrap::Unwrap<NodeVitastor>(parent);

    NodeVitastorImage *img = new NodeVitastorImage();
    img->Wrap(info.This());

    img->cli = cli;
    img->name = name;

    img->Ref();
    cli->Ref();
    vitastor_c_watch_inode(cli->c, (char*)img->name.c_str(), on_watch_start, img);

    info.GetReturnValue().Set(info.This());
}

NodeVitastorImage::~NodeVitastorImage()
{
    if (watch)
    {
        vitastor_c_close_watch(cli->c, watch);
        watch = NULL;
    }
    cli->Unref();
}

// read(offset, len, callback(err, buffer, version))
NAN_METHOD(NodeVitastorImage::Read)
{
    TRACE("NodeVitastorImage::Read");
    if (info.Length() < 3)
        Nan::ThrowError("Not enough arguments to read(offset, len, callback(err, buffer, version))");

    NodeVitastorImage* img = Nan::ObjectWrap::Unwrap<NodeVitastorImage>(info.This());

    auto req = img->cli->get_read_request(info, 0);
    req->img = img;
    req->op = NODE_VITASTOR_READ;

    img->exec_or_wait(req);
}

// write(offset, buffer, { version }?, callback(err))
NAN_METHOD(NodeVitastorImage::Write)
{
    TRACE("NodeVitastorImage::Write");
    if (info.Length() < 3)
        Nan::ThrowError("Not enough arguments to write(offset, buffer, { version }?, callback(err))");

    NodeVitastorImage* img = Nan::ObjectWrap::Unwrap<NodeVitastorImage>(info.This());

    auto req = img->cli->get_write_request(info, 0);
    req->img = img;
    req->op = NODE_VITASTOR_WRITE;

    img->exec_or_wait(req);
}

// sync(callback(err))
NAN_METHOD(NodeVitastorImage::Sync)
{
    TRACE("NodeVitastorImage::Sync");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to sync(callback(err))");

    NodeVitastorImage* img = Nan::ObjectWrap::Unwrap<NodeVitastorImage>(info.This());

    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    auto req = new NodeVitastorRequest(img->cli, callback);
    req->img = img;
    req->op = NODE_VITASTOR_SYNC;

    img->exec_or_wait(req);
}

// read_bitmap(offset, len, with_parents, callback(err, bitmap_buffer))
NAN_METHOD(NodeVitastorImage::ReadBitmap)
{
    TRACE("NodeVitastorImage::ReadBitmap");
    if (info.Length() < 4)
        Nan::ThrowError("Not enough arguments to read_bitmap(offset, len, with_parents, callback(err, bitmap_buffer))");

    NodeVitastorImage* img = Nan::ObjectWrap::Unwrap<NodeVitastorImage>(info.This());

    uint64_t offset = get_ui64(info[0]);
    uint64_t len = get_ui64(info[1]);
    bool with_parents = Nan::To<bool>(info[2]).FromJust();
    v8::Local<v8::Function> callback = info[3].As<v8::Function>();

    auto req = new NodeVitastorRequest(img->cli, callback);
    req->img = img;
    req->op = NODE_VITASTOR_READ_BITMAP;
    req->offset = offset;
    req->len = len;
    req->with_parents = with_parents;

    img->exec_or_wait(req);
}

// get_info(callback({ num, name, size, parent_id?, readonly?, meta?, mod_revision, block_size, bitmap_granularity, immediate_commit }))
NAN_METHOD(NodeVitastorImage::GetInfo)
{
    TRACE("NodeVitastorImage::GetInfo");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to get_info(callback({ num, name, size, parent_id?, readonly?, meta?, mod_revision, block_size, bitmap_granularity, immediate_commit }))");

    NodeVitastorImage* img = Nan::ObjectWrap::Unwrap<NodeVitastorImage>(info.This());

    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    auto req = new NodeVitastorRequest(img->cli, callback);
    req->img = img;
    req->op = NODE_VITASTOR_GET_INFO;

    img->exec_or_wait(req);
}

void NodeVitastorImage::exec_or_wait(NodeVitastorRequest *req)
{
    if (!watch)
    {
        // Need to wait for initialisation
        on_init.push_back(req);
    }
    else
    {
        exec_request(req);
    }
}

void NodeVitastorImage::exec_request(NodeVitastorRequest *req)
{
    if (req->op == NODE_VITASTOR_READ)
    {
        uint64_t ino = vitastor_c_inode_get_num(watch);
        cli->Ref();
        vitastor_c_read(cli->c, ino, req->offset, req->len, &req->iov, 1, NodeVitastor::on_read_finish, req);
    }
    else if (req->op == NODE_VITASTOR_WRITE)
    {
        uint64_t ino = vitastor_c_inode_get_num(watch);
        cli->Ref();
        vitastor_c_write(cli->c, ino, req->offset, req->len, req->version,
            req->iov_list.size() ? req->iov_list.data() : &req->iov,
            req->iov_list.size() ? req->iov_list.size() : 1,
            NodeVitastor::on_write_finish, req);
    }
    else if (req->op == NODE_VITASTOR_SYNC)
    {
        uint64_t ino = vitastor_c_inode_get_num(watch);
        uint32_t imm = vitastor_c_inode_get_immediate_commit(cli->c, ino);
        cli->Ref();
        if (imm != IMMEDIATE_ALL)
        {
            vitastor_c_sync(cli->c, NodeVitastor::on_write_finish, req);
        }
        else
        {
            NodeVitastor::on_write_finish(req, 0);
        }
    }
    else if (req->op == NODE_VITASTOR_READ_BITMAP)
    {
        uint64_t ino = vitastor_c_inode_get_num(watch);
        cli->Ref();
        vitastor_c_read_bitmap(cli->c, ino, req->offset, req->len, req->with_parents, NodeVitastor::on_read_bitmap_finish, req);
    }
    else if (req->op == NODE_VITASTOR_GET_INFO)
    {
        uint64_t size = vitastor_c_inode_get_size(watch);
        uint64_t num = vitastor_c_inode_get_num(watch);
        uint32_t block_size = vitastor_c_inode_get_block_size(cli->c, num);
        uint32_t bitmap_granularity = vitastor_c_inode_get_bitmap_granularity(cli->c, num);
        int readonly = vitastor_c_inode_get_readonly(watch);
        uint32_t immediate_commit = vitastor_c_inode_get_immediate_commit(cli->c, num);
        uint64_t parent_id = vitastor_c_inode_get_parent_id(watch);
        char *meta = vitastor_c_inode_get_meta(watch);
        uint64_t mod_revision = vitastor_c_inode_get_mod_revision(watch);

        Nan::HandleScope scope;

        v8::Local<v8::Object> res = Nan::New<v8::Object>();
        Nan::Set(res, Nan::New<v8::String>("pool_id").ToLocalChecked(), Nan::New<v8::Number>(INODE_POOL(num)));
        Nan::Set(res, Nan::New<v8::String>("inode_num").ToLocalChecked(), Nan::New<v8::Number>(INODE_NO_POOL(num)));
        if (size < ((uint64_t)1<<53))
            Nan::Set(res, Nan::New<v8::String>("size").ToLocalChecked(), Nan::New<v8::Number>(size));
        else
            Nan::Set(res, Nan::New<v8::String>("size").ToLocalChecked(), v8::BigInt::NewFromUnsigned(v8::Isolate::GetCurrent(), size));
        if (parent_id)
        {
            Nan::Set(res, Nan::New<v8::String>("parent_pool_id").ToLocalChecked(), Nan::New<v8::Number>(INODE_POOL(parent_id)));
            Nan::Set(res, Nan::New<v8::String>("parent_inode_num").ToLocalChecked(), Nan::New<v8::Number>(INODE_NO_POOL(parent_id)));
        }
        Nan::Set(res, Nan::New<v8::String>("readonly").ToLocalChecked(), Nan::New((bool)readonly));
        if (meta)
        {
            Nan::JSON nanJSON;
            Nan::Set(res, Nan::New<v8::String>("meta").ToLocalChecked(), nanJSON.Parse(Nan::New<v8::String>(meta).ToLocalChecked()).ToLocalChecked());
        }
        if (mod_revision < ((uint64_t)1<<53))
            Nan::Set(res, Nan::New<v8::String>("mod_revision").ToLocalChecked(), Nan::New<v8::Number>(mod_revision));
        else
            Nan::Set(res, Nan::New<v8::String>("mod_revision").ToLocalChecked(), v8::BigInt::NewFromUnsigned(v8::Isolate::GetCurrent(), mod_revision));
        Nan::Set(res, Nan::New<v8::String>("block_size").ToLocalChecked(), Nan::New(block_size));
        Nan::Set(res, Nan::New<v8::String>("bitmap_granularity").ToLocalChecked(), Nan::New(bitmap_granularity));
        Nan::Set(res, Nan::New<v8::String>("immediate_commit").ToLocalChecked(), Nan::New(immediate_commit));

        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[1];
        args[0] = res;
        nanCallback.Call(1, args, req);

        delete req;
    }
}

void NodeVitastorImage::on_watch_start(void *opaque, long retval)
{
    NodeVitastorImage *img = (NodeVitastorImage *)opaque;
    {
        img->watch = (void*)retval;
        auto on_init = std::move(img->on_init);
        for (auto req: on_init)
        {
            img->exec_request(req);
        }
    }
    img->Unref();
}

//////////////////////////////////////////////////
// NodeVitastorKV
//////////////////////////////////////////////////

Nan::Persistent<v8::Function> NodeVitastorKV::listing_class;

// constructor(node_vitastor)
NAN_METHOD(NodeVitastorKV::Create)
{
    TRACE("NodeVitastorKV::Create");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to new KV(client)");

    v8::Local<v8::Object> parent = info[0].As<v8::Object>();
    NodeVitastor *cli = Nan::ObjectWrap::Unwrap<NodeVitastor>(parent);

    NodeVitastorKV *kv = new NodeVitastorKV();
    kv->cli = cli;
    {
        kv->dbw = new vitastorkv_dbw_t((cluster_client_t*)vitastor_c_get_internal_client(cli->c));
    }

    kv->Wrap(info.This());
    cli->Ref();
    info.GetReturnValue().Set(info.This());
}

NodeVitastorKV::~NodeVitastorKV()
{
    delete dbw;
    cli->Unref();
}

// open(pool_id, inode_num, { ...config }, callback(err))
NAN_METHOD(NodeVitastorKV::Open)
{
    TRACE("NodeVitastorKV::Open");
    if (info.Length() < 4)
        Nan::ThrowError("Not enough arguments to open(pool_id, inode_num, { ...config }, callback(err))");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    uint64_t inode_id = INODE_WITH_POOL(get_ui64(info[0]), get_ui64(info[1]));

    v8::Local<v8::Object> jsParams = info[2].As<v8::Object>();
    v8::Local<v8::Array> keys = Nan::GetOwnPropertyNames(jsParams).ToLocalChecked();
    std::map<std::string, std::string> cfg;
    for (uint32_t i = 0; i < keys->Length(); i++)
    {
        auto key = Nan::Get(keys, i).ToLocalChecked();
        cfg[std::string(*Nan::Utf8String(key))] = std::string(*Nan::Utf8String(Nan::Get(jsParams, key).ToLocalChecked()));
    }

    v8::Local<v8::Function> callback = info[3].As<v8::Function>();
    auto req = new NodeVitastorRequest(kv->cli, callback);

    kv->Ref();
    kv->dbw->open(inode_id, cfg, [kv, req](int res)
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[1];
        args[0] = !res ? v8::Local<v8::Value>(Nan::Null()) : v8::Local<v8::Value>(Nan::New<v8::Int32>(res));
        nanCallback.Call(1, args, req);
        delete req;
        kv->Unref();
    });
}

// close(callback(err))
NAN_METHOD(NodeVitastorKV::Close)
{
    TRACE("NodeVitastorKV::Close");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to close(callback(err))");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    auto req = new NodeVitastorRequest(kv->cli, callback);

    kv->Ref();
    kv->dbw->close([kv, req]()
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        nanCallback.Call(0, NULL, req);
        delete req;
        kv->Unref();
    });
}

// set_config({ ...config })
NAN_METHOD(NodeVitastorKV::SetConfig)
{
    TRACE("NodeVitastorKV::SetConfig");
    if (info.Length() < 1)
        Nan::ThrowError("Not enough arguments to set_config({ ...config })");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    v8::Local<v8::Object> jsParams = info[0].As<v8::Object>();
    v8::Local<v8::Array> keys = Nan::GetOwnPropertyNames(jsParams).ToLocalChecked();
    std::map<std::string, std::string> cfg;
    for (uint32_t i = 0; i < keys->Length(); i++)
    {
        auto key = Nan::Get(keys, i).ToLocalChecked();
        cfg[std::string(*Nan::Utf8String(key))] = std::string(*Nan::Utf8String(Nan::Get(jsParams, key).ToLocalChecked()));
    }

    kv->dbw->set_config(cfg);
}

// get_size()
NAN_METHOD(NodeVitastorKV::GetSize)
{
    TRACE("NodeVitastorKV::GetSize");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    auto size = kv->dbw->get_size();
    info.GetReturnValue().Set((size < ((uint64_t)1<<53))
        ? v8::Local<v8::Value>(Nan::New<v8::Number>(size))
        : v8::Local<v8::Value>(v8::BigInt::NewFromUnsigned(info.GetIsolate(), size)));
}

void NodeVitastorKV::get_impl(const Nan::FunctionCallbackInfo<v8::Value> & info, bool allow_cache)
{
    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    // FIXME: Handle Buffer too
    std::string key(*Nan::Utf8String(info[0].As<v8::String>()));

    v8::Local<v8::Function> callback = info[1].As<v8::Function>();
    auto req = new NodeVitastorRequest(kv->cli, callback);

    kv->Ref();
    kv->dbw->get(key, [kv, req](int res, const std::string & value)
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[2];
        args[0] = !res ? v8::Local<v8::Value>(Nan::Null()) : v8::Local<v8::Value>(Nan::New<v8::Int32>(res));
        args[1] = !res ? v8::Local<v8::Value>(Nan::New<v8::String>(value).ToLocalChecked()) : v8::Local<v8::Value>(Nan::Null());
        nanCallback.Call(2, args, req);
        delete req;
        kv->Unref();
    }, allow_cache);
}

// get(key, callback(err, value))
NAN_METHOD(NodeVitastorKV::Get)
{
    TRACE("NodeVitastorKV::Get");
    if (info.Length() < 2)
        Nan::ThrowError("Not enough arguments to get(key, callback(err, value))");
    get_impl(info, false);
}

// get_cached(key, callback(err, value))
NAN_METHOD(NodeVitastorKV::GetCached)
{
    TRACE("NodeVitastorKV::GetCached");
    if (info.Length() < 2)
        Nan::ThrowError("Not enough arguments to get_cached(key, callback(err, value))");
    get_impl(info, true);
}

static std::function<bool(int, const std::string &)> make_cas_callback(NodeVitastorRequest *cas_req)
{
    return [cas_req](int res, const std::string & value)
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(cas_req->callback));
        v8::Local<v8::Value> args[1];
        args[0] = !res ? v8::Local<v8::Value>(Nan::New<v8::String>(value).ToLocalChecked()) : v8::Local<v8::Value>(Nan::Null());
        Nan::MaybeLocal<v8::Value> ret = nanCallback.Call(1, args, cas_req);
        if (ret.IsEmpty())
            return false;
        return Nan::To<bool>(ret.ToLocalChecked()).FromJust();
    };
}

// set(key, value, callback(err), cas_compare(old_value)?)
NAN_METHOD(NodeVitastorKV::Set)
{
    TRACE("NodeVitastorKV::Set");
    if (info.Length() < 3)
        Nan::ThrowError("Not enough arguments to set(key, value, callback(err), cas_compare(old_value)?)");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    // FIXME: Handle Buffer too
    std::string key(*Nan::Utf8String(info[0].As<v8::String>()));
    std::string value(*Nan::Utf8String(info[1].As<v8::String>()));

    v8::Local<v8::Function> callback = info[2].As<v8::Function>();
    NodeVitastorRequest *req = new NodeVitastorRequest(kv->cli, callback), *cas_req = NULL;

    std::function<bool(int, const std::string &)> cas_cb;
    if (info.Length() > 3 && info[3]->IsObject())
    {
        v8::Local<v8::Function> cas_callback = info[3].As<v8::Function>();
        cas_req = new NodeVitastorRequest(kv->cli, cas_callback);
        cas_cb = make_cas_callback(cas_req);
    }

    kv->Ref();
    kv->dbw->set(key, value, [kv, req, cas_req](int res)
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[1];
        args[0] = !res ? v8::Local<v8::Value>(Nan::Null()) : v8::Local<v8::Value>(Nan::New<v8::Int32>(res));
        nanCallback.Call(1, args, req);
        delete req;
        if (cas_req)
            delete cas_req;
        kv->Unref();
    }, cas_cb);
}

// del(key, callback(err), cas_compare(old_value)?)
NAN_METHOD(NodeVitastorKV::Del)
{
    TRACE("NodeVitastorKV::Del");
    if (info.Length() < 2)
        Nan::ThrowError("Not enough arguments to del(key, callback(err), cas_compare(old_value)?)");

    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());

    // FIXME: Handle Buffer too
    std::string key(*Nan::Utf8String(info[0].As<v8::String>()));

    v8::Local<v8::Function> callback = info[1].As<v8::Function>();
    NodeVitastorRequest *req = new NodeVitastorRequest(kv->cli, callback), *cas_req = NULL;

    std::function<bool(int, const std::string &)> cas_cb;
    if (info.Length() > 2 && info[2]->IsObject())
    {
        v8::Local<v8::Function> cas_callback = info[2].As<v8::Function>();
        cas_req = new NodeVitastorRequest(kv->cli, cas_callback);
        cas_cb = make_cas_callback(cas_req);
    }

    kv->Ref();
    kv->dbw->del(key, [kv, req, cas_req](int res)
    {
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[1];
        args[0] = !res ? v8::Local<v8::Value>(Nan::Null()) : v8::Local<v8::Value>(Nan::New<v8::Int32>(res));
        nanCallback.Call(1, args, req);
        delete req;
        if (cas_req)
            delete cas_req;
        kv->Unref();
    }, cas_cb);
}

// list(start_key?)
NAN_METHOD(NodeVitastorKV::List)
{
    TRACE("NodeVitastorKV::List");

    v8::Local<v8::Function> cons = Nan::New(listing_class);
    v8::Local<v8::Value> args[2];
    args[0] = info.This();
    int narg = 1;
    if (info.Length() > 1 && info[1]->IsString())
    {
        args[1] = info[1];
        narg = 2;
    }
    info.GetReturnValue().Set(Nan::NewInstance(cons, narg, args).ToLocalChecked());
}

/*NAN_METHOD(NodeVitastorKV::Destroy)
{
    TRACE("NodeVitastorKV::Destroy");
    NodeVitastorKV* kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(info.This());
    if (!kv->dead)
        kv->Unref();
}*/

//////////////////////////////////////////////////
// NodeVitastorKVListing
//////////////////////////////////////////////////

// constructor(node_vitastor_kv, start_key?)
NAN_METHOD(NodeVitastorKVListing::Create)
{
    TRACE("NodeVitastorKVListing::Create");

    v8::Local<v8::Object> parent = info[0].As<v8::Object>();
    NodeVitastorKV *kv = Nan::ObjectWrap::Unwrap<NodeVitastorKV>(parent);

    std::string start_key;
    // FIXME: Handle Buffer too
    if (info.Length() > 1 && info[1]->IsString())
    {
        start_key = std::string(*Nan::Utf8String(info[1].As<v8::String>()));
    }

    NodeVitastorKVListing *list = new NodeVitastorKVListing();
    list->kv = kv;
    list->handle = list->kv->dbw->list_start(start_key);

    list->Wrap(info.This());
    kv->Ref();
    info.GetReturnValue().Set(info.This());
}

NodeVitastorKVListing::~NodeVitastorKVListing()
{
    if (handle)
    {
        kv->dbw->list_close(handle);
        handle = NULL;
    }
    if (iter)
    {
        delete iter;
        iter = NULL;
    }
    kv->Unref();
}

// next(callback(err, value)?)
NAN_METHOD(NodeVitastorKVListing::Next)
{
    TRACE("NodeVitastorKVListing::Next");

    NodeVitastorKVListing* list = Nan::ObjectWrap::Unwrap<NodeVitastorKVListing>(info.This());

    if (info.Length() > 0)
    {
        v8::Local<v8::Function> callback = info[0].As<v8::Function>();
        if (list->iter)
        {
            delete list->iter;
        }
        list->iter = new NodeVitastorRequest(list->kv->cli, callback);
    }
    if (!list->handle)
    {
        // Already closed
        if (list->iter)
        {
            auto req = list->iter;
            list->iter = NULL;
            Nan::Callback nanCallback(Nan::New(req->callback));
            v8::Local<v8::Value> args[1];
            args[0] = Nan::New<v8::Int32>(-EINVAL);
            nanCallback.Call(1, args, req);
            delete req;
        }
        return;
    }

    list->kv->Ref();
    list->kv->dbw->list_next(list->handle, [list](int res, const std::string & key, const std::string & value)
    {
        auto req = list->iter;
        list->iter = NULL;
        Nan::HandleScope scope;
        Nan::Callback nanCallback(Nan::New(req->callback));
        v8::Local<v8::Value> args[3];
        args[0] = Nan::New<v8::Int32>(res);
        args[1] = !res ? v8::Local<v8::Value>(Nan::New<v8::String>(key).ToLocalChecked()) : v8::Local<v8::Value>(Nan::Null());
        args[2] = !res ? v8::Local<v8::Value>(Nan::New<v8::String>(value).ToLocalChecked()) : v8::Local<v8::Value>(Nan::Null());
        nanCallback.Call(3, args, req);
        if (list->iter)
            delete req;
        else
            list->iter = req;
        list->kv->Unref();
    });
}

// close()
NAN_METHOD(NodeVitastorKVListing::Close)
{
    TRACE("NodeVitastorKVListing::Close");

    NodeVitastorKVListing* list = Nan::ObjectWrap::Unwrap<NodeVitastorKVListing>(info.This());

    if (list->handle)
    {
        list->kv->dbw->list_close(list->handle);
        list->handle = NULL;
    }
    if (list->iter)
    {
        delete list->iter;
        list->iter = NULL;
    }
}
