// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>
#include "etcd_state_client_mock.h"
#include "str_util.h"

etcd_state_client_mock_t::etcd_state_client_mock_t()
{
    timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
}

void etcd_state_client_mock_t::etcd_add_watch(json11::Json watch)
{
}

void etcd_state_client_mock_t::etcd_call_oneshot(std::string etcd_address, std::string api, json11::Json payload,
    int timeout, std::function<void(std::string, json11::Json)> callback)
{
}

void etcd_state_client_mock_t::pause()
{
    paused = true;
}

void etcd_state_client_mock_t::resume()
{
    paused = false;
    auto queue = std::move(this->queue);
    for (auto& req: queue)
    {
        etcd_call(req.api, req.payload, req.timeout, req.retries, req.interval, req.callback);
    }
}

void etcd_state_client_mock_t::set(const std::string& key, json11::Json data, uint64_t mod_revision, uint64_t lease_id)
{
    if (!mod_revision)
        mod_revision = ++this->mod_revision;
    this->data[key] = (etcd_mock_key_data_t){ .value = data.dump(), .mod_revision = mod_revision, .lease_id = lease_id };
}

void etcd_state_client_mock_t::etcd_call(std::string api, json11::Json payload, int timeout,
    int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    if (paused)
    {
        queue.push_back({ api, payload, timeout, retries, interval, callback });
        return;
    }
    printf("+ etcd: %s %s\n", api.c_str(), payload.dump().c_str());
    if (api == "/kv/txn")
    {
        bool ok = true;
        for (auto& check: payload["compare"].array_items())
        {
            auto key = base64_decode(check["key"].string_value());
            etcd_mock_key_data_t *key_data = data.find(key) != data.end() ? &data.at(key) : NULL;
            auto target = check["target"].string_value();
            auto res = check["result"].string_value();
            assert(res == "LESS" || res == "");
            bool less = res == "LESS";
            if (target == "MOD")
            {
                uint64_t rev = check["mod_revision"].uint64_value();
                assert(!less || rev);
                ok = ok && (less ? (!key_data || key_data->mod_revision < rev) : (key_data && key_data->mod_revision == rev));
            }
            else if (target == "CREATE")
            {
                uint64_t rev = check["create_revision"].uint64_value();
                assert(rev == 0 && !less);
                ok = ok && !key_data;
            }
            else if (target == "VERSION")
            {
                uint64_t rev = check["version"].uint64_value();
                assert(rev == 0 && !less);
                ok = ok && !key_data;
            }
            else if (target == "LEASE")
            {
                assert(!less);
                uint64_t lease_id = check["lease"].uint64_value();
                ok = ok && key_data && key_data->lease_id == lease_id;
            }
            else
                assert(0);
        }
        std::map<std::string, etcd_kv_t> changes;
        bool has_mod = false;
        for (auto& op: payload[ok ? "success" : "failure"].array_items())
        {
            auto& obj = op.object_items();
            has_mod = has_mod || obj.find("request_put") != obj.end() ||
                obj.find("request_delete_range") != obj.end();
        }
        if (has_mod)
        {
            mod_revision++;
        }
        json11::Json::array responses;
        for (auto& op_ptr: payload[ok ? "success" : "failure"].array_items())
        {
            auto& op = op_ptr.object_items();
            if (op.find("request_range") != op.end())
            {
                json11::Json::array kvs;
                auto req = op.at("request_range");
                auto key = base64_decode(req["key"].string_value());
                auto range_end = base64_decode(req["range_end"].string_value());
                auto begin_it = range_end.empty() ? data.find(key) : data.lower_bound(key);
                auto end_it = range_end.empty() ? (begin_it == data.end() ? begin_it : std::next(begin_it)) : data.lower_bound(range_end);
                for (auto it = begin_it; it != end_it; it++)
                {
                    printf("\\- get: %s = %s, rev %ju\n", it->first.c_str(), it->second.value.c_str(), it->second.mod_revision);
                    kvs.push_back(json11::Json::object {
                        { "key", base64_encode(it->first) },
                        { "value", base64_encode(it->second.value) },
                        { "mod_revision", it->second.mod_revision },
                    });
                }
                responses.push_back(json11::Json::object {
                    { "response_range", json11::Json::object{ { "header", json11::Json::object{ { "revision", mod_revision } } }, { "kvs", kvs } } },
                });
            }
            else if (op.find("request_put") != op.end())
            {
                auto req = op.at("request_put");
                auto key = base64_decode(req["key"].string_value());
                auto value = base64_decode(req["value"].string_value());
                auto lease_id = req["lease"].uint64_value();
                printf("\\- put: %s = %s, rev %ju, lease %ju\n", key.c_str(), value.c_str(), mod_revision, lease_id);
                data[key] = {
                    .value = value,
                    .mod_revision = mod_revision,
                    .lease_id = lease_id,
                };
                std::string err;
                json11::Json json_value = json11::Json::parse(value, err);
                if (err != "")
                {
                    fprintf(stderr, "Invalid JSON in etcd key %s during test: %s\n", key.c_str(), value.c_str());
                    exit(1);
                }
                changes[key] = { .key = key, .value = json_value, .mod_revision = mod_revision };
                responses.push_back(json11::Json::object {
                    { "response_put", json11::Json::object{ { "header", json11::Json::object{ { "revision", mod_revision } } } } },
                });
            }
            else if (op.find("request_delete_range") != op.end())
            {
                auto req = op.at("request_delete_range");
                auto key = base64_decode(req["key"].string_value());
                auto range_end = base64_decode(req["range_end"].string_value());
                uint64_t n_del = 0;
                for (auto it = data.lower_bound(key); it != data.end() && (range_end == "" || it->first < range_end); )
                {
                    auto & key = it->first;
                    printf("\\- del: %s\n", key.c_str());
                    changes[key] = { .key = key, .mod_revision = mod_revision };
                    n_del++;
                    data.erase(it++);
                }
                responses.push_back(json11::Json::object {
                    { "response_delete_range", json11::Json::object{ { "header", json11::Json::object{ { "revision", mod_revision } } }, { "deleted", n_del } } },
                });
            }
        }
        callback("", json11::Json::object{
            { "header", json11::Json::object{ { "revision", mod_revision } } },
            { "succeeded", ok },
            { "responses", responses }
        });
        // Push changes to watcher
        if (changes.size())
        {
            for (auto & kv: changes)
                parse_state(kv.second);
            if (on_change_hook != NULL)
                on_change_hook(changes);
        }
    }
    else if (api == "/lease/grant")
    {
        uint64_t lease_id = (((uint64_t)lrand48()) << 32) | lrand48();
        leases[lease_id] = payload["TTL"].uint64_value();
        callback("", json11::Json::object{ { "ID", std::to_string(lease_id) } });
    }
    else
        callback("Unsupported", json11::Json());
}

void etcd_state_client_mock_t::load_global_config()
{
    etcd_state_client_t::load_global_config([this](const std::string & err) {});
}

void etcd_state_client_mock_t::load_pgs()
{
    etcd_state_client_t::load_pgs([this](const std::string & err) {});
}
