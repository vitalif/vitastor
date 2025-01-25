// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "addon.h"

// Initialize the node addon
NAN_MODULE_INIT(InitAddon)
{
    // vitastor.Client

    v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(NodeVitastor::Create);
    tpl->SetClassName(Nan::New("Client").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "read", NodeVitastor::Read);
    Nan::SetPrototypeMethod(tpl, "write", NodeVitastor::Write);
    Nan::SetPrototypeMethod(tpl, "sync", NodeVitastor::Sync);
    Nan::SetPrototypeMethod(tpl, "read_bitmap", NodeVitastor::ReadBitmap);
    Nan::SetPrototypeMethod(tpl, "on_ready", NodeVitastor::OnReady);
    Nan::SetPrototypeMethod(tpl, "get_min_io_size", NodeVitastor::GetMinIoSize);
    Nan::SetPrototypeMethod(tpl, "get_max_atomic_write_size", NodeVitastor::GetMaxAtomicWriteSize);
    Nan::SetPrototypeMethod(tpl, "get_immediate_commit", NodeVitastor::GetImmediateCommit);
    //Nan::SetPrototypeMethod(tpl, "destroy", NodeVitastor::Destroy);

    Nan::Set(target, Nan::New("Client").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());

    // vitastor.Image (opened image)

    tpl = Nan::New<v8::FunctionTemplate>(NodeVitastorImage::Create);
    tpl->SetClassName(Nan::New("Image").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "read", NodeVitastorImage::Read);
    Nan::SetPrototypeMethod(tpl, "write", NodeVitastorImage::Write);
    Nan::SetPrototypeMethod(tpl, "sync", NodeVitastorImage::Sync);
    Nan::SetPrototypeMethod(tpl, "get_info", NodeVitastorImage::GetInfo);
    Nan::SetPrototypeMethod(tpl, "read_bitmap", NodeVitastorImage::ReadBitmap);

    Nan::Set(target, Nan::New("Image").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());

    // vitastor.KV

    tpl = Nan::New<v8::FunctionTemplate>(NodeVitastorKV::Create);
    tpl->SetClassName(Nan::New("KV").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "open", NodeVitastorKV::Open);
    Nan::SetPrototypeMethod(tpl, "set_config", NodeVitastorKV::SetConfig);
    Nan::SetPrototypeMethod(tpl, "close", NodeVitastorKV::Close);
    Nan::SetPrototypeMethod(tpl, "get_size", NodeVitastorKV::GetSize);
    Nan::SetPrototypeMethod(tpl, "get", NodeVitastorKV::Get);
    Nan::SetPrototypeMethod(tpl, "get_cached", NodeVitastorKV::GetCached);
    Nan::SetPrototypeMethod(tpl, "set", NodeVitastorKV::Set);
    Nan::SetPrototypeMethod(tpl, "del", NodeVitastorKV::Del);
    Nan::SetPrototypeMethod(tpl, "list", NodeVitastorKV::List);

    Nan::Set(target, Nan::New("KV").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());

    Nan::Set(target, Nan::New("ENOENT").ToLocalChecked(), Nan::New<v8::Int32>(-ENOENT));
    Nan::Set(target, Nan::New("EIO").ToLocalChecked(), Nan::New<v8::Int32>(-EIO));
    Nan::Set(target, Nan::New("EINVAL").ToLocalChecked(), Nan::New<v8::Int32>(-EINVAL));
    Nan::Set(target, Nan::New("EROFS").ToLocalChecked(), Nan::New<v8::Int32>(-EROFS));
    Nan::Set(target, Nan::New("ENOSPC").ToLocalChecked(), Nan::New<v8::Int32>(-ENOSPC));
    Nan::Set(target, Nan::New("EINTR").ToLocalChecked(), Nan::New<v8::Int32>(-EINTR));
    Nan::Set(target, Nan::New("EILSEQ").ToLocalChecked(), Nan::New<v8::Int32>(-EILSEQ));
    Nan::Set(target, Nan::New("ENOTBLK").ToLocalChecked(), Nan::New<v8::Int32>(-ENOTBLK));
    Nan::Set(target, Nan::New("ENOSYS").ToLocalChecked(), Nan::New<v8::Int32>(-ENOSYS));
    Nan::Set(target, Nan::New("EAGAIN").ToLocalChecked(), Nan::New<v8::Int32>(-EAGAIN));

    Nan::Set(target, Nan::New("IMMEDIATE_NONE").ToLocalChecked(), Nan::New<v8::Int32>(IMMEDIATE_NONE));
    Nan::Set(target, Nan::New("IMMEDIATE_SMALL").ToLocalChecked(), Nan::New<v8::Int32>(IMMEDIATE_SMALL));
    Nan::Set(target, Nan::New("IMMEDIATE_ALL").ToLocalChecked(), Nan::New<v8::Int32>(IMMEDIATE_ALL));

    // Listing handle

    tpl = Nan::New<v8::FunctionTemplate>(NodeVitastorKVListing::Create);
    tpl->SetClassName(Nan::New("KVListing").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "next", NodeVitastorKVListing::Next);
    Nan::SetPrototypeMethod(tpl, "close", NodeVitastorKVListing::Close);

    Nan::Set(target, Nan::New("KVListing").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());

    NodeVitastorKV::listing_class.Reset(Nan::GetFunction(tpl).ToLocalChecked());
}

NODE_MODULE(addon, (void*)InitAddon)
