// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "etcd_state_client_http.h"
#include "addr_util.h"
#include "http_client.h"
#include "str_util.h"

etcd_state_client_http_t::etcd_state_client_http_t(timerfd_manager_t *tfd)
{
    this->tfd = tfd;
}

etcd_state_client_http_t::~etcd_state_client_http_t()
{
    stop_ws_keepalive();
    if (etcd_watch_ws)
    {
        http_close(etcd_watch_ws);
        etcd_watch_ws = NULL;
    }
    if (keepalive_client)
    {
        http_close(keepalive_client);
        keepalive_client = NULL;
    }
    if (load_pgs_timer_id >= 0)
    {
        tfd->clear_timer(load_pgs_timer_id);
        load_pgs_timer_id = -1;
    }
    etcd_watches_initialised = -1;
}

void etcd_state_client_http_t::etcd_add_watch(json11::Json watch)
{
    if (etcd_watch_ws)
    {
        http_post_message(etcd_watch_ws, WS_TEXT, watch.dump());
    }
}

void etcd_state_client_http_t::etcd_call_oneshot(std::string etcd_address, std::string api, json11::Json payload,
    int timeout, std::function<void(std::string, json11::Json)> callback)
{
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    std::string req = payload.dump();
    req = "POST "+etcd_api_path+api+" HTTP/1.1\r\n"
        "Host: "+etcd_address+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: close\r\n"
        "\r\n"+req;
    auto http_cli = http_init(tfd);
    auto cb = [http_cli, callback](const http_response_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        callback(err, data);
        http_close(http_cli);
    };
    http_request(http_cli, etcd_address, req, { .timeout = timeout }, cb);
}

void etcd_state_client_http_t::etcd_call(std::string api, json11::Json payload, int timeout,
    int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    if (!etcd_addresses.size() && !etcd_local.size())
    {
        fprintf(stderr, "etcd_address is missing in Vitastor configuration\n");
        exit(1);
    }
    pick_next_etcd();
    std::string etcd_address = selected_etcd_address;
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    std::string req = payload.dump();
    req = "POST "+etcd_api_path+api+" HTTP/1.1\r\n"
        "Host: "+etcd_address+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: keep-alive\r\n"
        "Keep-Alive: timeout="+std::to_string(etcd_keepalive_timeout)+"\r\n"
        "\r\n"+req;
    retries--;
    auto cb = [this, api, payload, timeout, retries, interval, callback,
        cur_addr = selected_etcd_address](const http_response_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        if (err != "")
        {
            if (cur_addr == selected_etcd_address)
                selected_etcd_address = "";
            if (retries > 0)
            {
                if (this->log_level > 0)
                {
                    fprintf(
                        stderr, "Warning: etcd request failed: %s, retrying %d more times\n",
                        err.c_str(), retries
                    );
                }
                if (interval > 0)
                {
                    // FIXME: Prevent destruction of etcd_state_client if timers or requests are active
                    tfd->set_timer(interval, false, [this, api, payload, timeout, retries, interval, callback](int)
                    {
                        etcd_call(api, payload, timeout, retries, interval, callback);
                    });
                }
                else
                    etcd_call(api, payload, timeout, retries, interval, callback);
            }
            else
                callback(err, data);
        }
        else
            callback(err, data);
    };
    if (!keepalive_client)
    {
        keepalive_client = http_init(tfd);
    }
    http_request(keepalive_client, etcd_address, req, { .timeout = timeout, .keepalive = true }, cb);
}

void etcd_state_client_http_t::parse_config(const json11::Json & config)
{
    auto old_etcd_ws_keepalive_interval = this->etcd_ws_keepalive_interval;
    etcd_state_client_t::parse_config(config);
    if (this->etcd_ws_keepalive_interval != old_etcd_ws_keepalive_interval && ws_keepalive_timer >= 0)
    {
        stop_ws_keepalive();
        start_ws_keepalive();
    }
}

void etcd_state_client_http_t::pick_next_etcd()
{
    if (selected_etcd_address != "")
        return;
    if (addresses_to_try.size() == 0)
    {
        // Prefer local etcd, if any
        for (int i = 0; i < etcd_local.size(); i++)
            addresses_to_try.push_back(etcd_local[i]);
        std::vector<int> ns;
        for (int i = 0; i < etcd_addresses.size(); i++)
            ns.push_back(i);
        if (!rand_initialized)
        {
            timespec tv;
            clock_gettime(CLOCK_REALTIME, &tv);
            srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
            rand_initialized = true;
        }
        while (ns.size())
        {
            int i = lrand48() % ns.size();
            addresses_to_try.push_back(etcd_addresses[ns[i]]);
            ns.erase(ns.begin()+i, ns.begin()+i+1);
        }
    }
    selected_etcd_address = addresses_to_try[0];
    addresses_to_try.erase(addresses_to_try.begin(), addresses_to_try.begin()+1);
}

void etcd_state_client_http_t::start_etcd_watcher()
{
    if (!etcd_addresses.size() && !etcd_local.size())
    {
        fprintf(stderr, "etcd_address is missing in Vitastor configuration\n");
        exit(1);
    }
    pick_next_etcd();
    std::string etcd_address = selected_etcd_address;
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    etcd_watches_initialised = 0;
    ws_alive = 1;
    if (etcd_watch_ws)
    {
        http_close(etcd_watch_ws);
        etcd_watch_ws = NULL;
    }
    if (this->log_level > 1)
    {
        fprintf(stderr, "Trying to connect to etcd websocket at %s, watch from revision %ju/%ju/%ju\n", etcd_address.c_str(),
            etcd_watch_revision_config, etcd_watch_revision_osd, etcd_watch_revision_pg);
    }
    etcd_watch_ws = open_websocket(tfd, etcd_address, etcd_api_path+"/watch", etcd_slow_timeout,
        [this, cur_addr = selected_etcd_address](const http_response_t *msg)
    {
        if (msg->body.length())
        {
            ws_alive = 1;
            std::string json_err;
            json11::Json data = json11::Json::parse(msg->body, json_err);
            if (json_err != "")
            {
                fprintf(stderr, "Bad JSON in etcd event: %s, ignoring event\n", json_err.c_str());
            }
            else
            {
                uint64_t watch_id = data["result"]["watch_id"].uint64_value();
                if (data["result"]["created"].bool_value())
                {
                    if (watch_id == ETCD_CONFIG_WATCH_ID ||
                        watch_id == ETCD_PG_STATE_WATCH_ID ||
                        watch_id == ETCD_OSD_STATE_WATCH_ID)
                    {
                        etcd_watches_initialised++;
                    }
                    if (etcd_watches_initialised == ETCD_TOTAL_WATCHES && this->log_level > 0)
                    {
                        fprintf(stderr, "Successfully subscribed to etcd at %s, revision %ju/%ju/%ju\n", cur_addr.c_str(),
                            etcd_watch_revision_config, etcd_watch_revision_osd, etcd_watch_revision_pg);
                    }
                }
                if (data["result"]["canceled"].bool_value())
                {
                    // etcd watch canceled, maybe because the revision was compacted
                    if (data["result"]["compact_revision"].uint64_value())
                    {
                        // we may miss events if we proceed
                        // so we should restart from the beginning if we can
                        if (on_reload_hook != NULL)
                        {
                            // check to not trigger on_reload_hook multiple times
                            if (etcd_watch_ws != NULL)
                            {
                                fprintf(stderr, "Revisions before %ju were compacted by etcd, reloading state\n",
                                    data["result"]["compact_revision"].uint64_value());
                                http_close(etcd_watch_ws);
                                etcd_watch_ws = NULL;
                                etcd_watch_revision_config = etcd_watch_revision_osd = etcd_watch_revision_pg = 0;
                                on_reload_hook();
                            }
                            return;
                        }
                        else
                        {
                            fprintf(stderr, "Revisions before %ju were compacted by etcd, exiting\n",
                                data["result"]["compact_revision"].uint64_value());
                            exit(1);
                        }
                    }
                    else
                    {
                        fprintf(stderr, "Watch canceled by etcd, reason: %s, exiting\n", data["result"]["cancel_reason"].string_value().c_str());
                        exit(1);
                    }
                }
                // Save revision only if it's present in the message - because sometimes etcd sends something without a header, like:
                // {"error": {"grpc_code": 14, "http_code": 503, "http_status": "Service Unavailable", "message": "error reading from server: EOF"}}
                // Also don't save revision from the initial created: true messages because they always contain the latest revision
                if (etcd_watches_initialised == ETCD_TOTAL_WATCHES &&
                    !data["result"]["header"]["revision"].is_null() &&
                    !data["result"]["created"].bool_value())
                {
                    // Restart watchers from the same revision number as in the last received message,
                    // not from the next one to protect against revision being split into multiple messages,
                    // even though etcd guarantees not to do that **within a single watcher** without fragment=true:
                    // https://etcd.io/docs/v3.5/learning/api_guarantees/#watch-apis
                    // Revision contents are ALWAYS split into separate messages for different watchers though!
                    // So generally we have to resume each watcher from its own revision...
                    // Progress messages may have watch_id=-1 if sent on behalf of multiple watchers though.
                    // And antietcd has an advanced semantic which merges the same revision for all watchers
                    // into one message and just omits watch_id.
                    // So we also have to handle the case where watch_id is -1 or not present (0).
                    auto watch_rev = data["result"]["header"]["revision"].uint64_value();
                    if (!watch_id || watch_id == UINT64_MAX)
                        etcd_watch_revision_config = etcd_watch_revision_osd = etcd_watch_revision_pg = watch_rev;
                    else if (watch_id == ETCD_CONFIG_WATCH_ID)
                        etcd_watch_revision_config = watch_rev;
                    else if (watch_id == ETCD_PG_STATE_WATCH_ID)
                        etcd_watch_revision_pg = watch_rev;
                    else if (watch_id == ETCD_OSD_STATE_WATCH_ID)
                        etcd_watch_revision_osd = watch_rev;
                    addresses_to_try.clear();
                }
                // First gather all changes into a hash to remove multiple overwrites
                std::map<std::string, etcd_kv_t> changes;
                for (auto & ev: data["result"]["events"].array_items())
                {
                    auto kv = parse_etcd_kv(ev["kv"]);
                    if (kv.key != "")
                    {
                        changes[kv.key] = kv;
                    }
                }
                for (auto & kv: changes)
                {
                    if (this->log_level > 3)
                    {
                        fprintf(stderr, "Incoming event: %s -> %s\n", kv.first.c_str(), kv.second.value.dump().c_str());
                    }
                    parse_state(kv.second);
                }
                // React to changes
                if (on_change_hook != NULL)
                {
                    on_change_hook(changes);
                }
            }
        }
        if (msg->eof)
        {
            fprintf(stderr, "Disconnected from etcd %s\n", cur_addr.c_str());
            if (cur_addr == selected_etcd_address)
                selected_etcd_address = "";
            if (etcd_watch_ws)
            {
                http_close(etcd_watch_ws);
                etcd_watch_ws = NULL;
            }
            if (etcd_watches_initialised == 0)
            {
                // Connection not established, retry in <etcd_quick_timeout>
                tfd->set_timer(etcd_quick_timeout, false, [this](int)
                {
                    start_etcd_watcher();
                });
            }
            else if (etcd_watches_initialised > 0)
            {
                // Connection was live, retry immediately
                etcd_watches_initialised = 0;
                start_etcd_watcher();
            }
        }
    });
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/config/") },
            { "range_end", base64_encode(etcd_prefix+"/config0") },
            { "start_revision", etcd_watch_revision_config },
            { "watch_id", ETCD_CONFIG_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/osd/state/") },
            { "range_end", base64_encode(etcd_prefix+"/osd/state0") },
            { "start_revision", etcd_watch_revision_osd },
            { "watch_id", ETCD_OSD_STATE_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/pg/") },
            { "range_end", base64_encode(etcd_prefix+"/pg0") },
            { "start_revision", etcd_watch_revision_pg },
            { "watch_id", ETCD_PG_STATE_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    // FIXME: Do not watch /pg/history/ at all in client code (not in OSD)
    if (on_start_watcher_hook)
    {
        on_start_watcher_hook(etcd_watch_ws);
    }
    start_ws_keepalive();
}

void etcd_state_client_http_t::stop_ws_keepalive()
{
    if (ws_keepalive_timer >= 0)
    {
        tfd->clear_timer(ws_keepalive_timer);
        ws_keepalive_timer = -1;
    }
}

void etcd_state_client_http_t::start_ws_keepalive()
{
    if (ws_keepalive_timer < 0)
    {
        ws_keepalive_timer = tfd->set_timer(etcd_ws_keepalive_interval*1000, true, [this](int)
        {
            if (!etcd_watch_ws || etcd_watches_initialised < ETCD_TOTAL_WATCHES)
            {
                // Do nothing
            }
            else if (!ws_alive)
            {
                if (this->log_level > 0)
                {
                    fprintf(stderr, "Websocket ping failed, disconnecting from etcd %s\n", selected_etcd_address.c_str());
                }
                if (etcd_watch_ws)
                {
                    http_close(etcd_watch_ws);
                    etcd_watch_ws = NULL;
                }
                start_etcd_watcher();
            }
            else
            {
                ws_alive = 0;
                http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
                    { "progress_request", json11::Json::object { } }
                }).dump());
            }
        });
    }
}

void etcd_state_client_http_t::load_global_config()
{
    etcd_state_client_t::load_global_config([this](const std::string & err)
    {
        if (err != "")
        {
            fprintf(stderr, "Error reading configuration from etcd: %s\n", err.c_str());
            if (infinite_start)
            {
                tfd->set_timer(etcd_slow_timeout, false, [this](int timer_id)
                {
                    load_global_config();
                });
            }
            else
            {
                exit(1);
            }
        }
    });
}

void etcd_state_client_http_t::load_pgs()
{
    timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    uint64_t ms_passed = (tv.tv_sec-etcd_last_reload.tv_sec)*1000 + (tv.tv_nsec-etcd_last_reload.tv_nsec)/1000000;
    if (ms_passed < etcd_min_reload_interval)
    {
        if (load_pgs_timer_id < 0)
        {
            load_pgs_timer_id = tfd->set_timer(etcd_min_reload_interval+50-ms_passed, false, [this](int) { load_pgs(); });
        }
        return;
    }
    etcd_last_reload = tv;
    if (load_pgs_timer_id >= 0)
    {
        tfd->clear_timer(load_pgs_timer_id);
        load_pgs_timer_id = -1;
    }
    etcd_state_client_t::load_pgs([this](const std::string & err)
    {
        if (err != "")
        {
            // Retry indefinitely
            fprintf(stderr, "Error loading PGs from etcd: %s\n", err.c_str());
            tfd->set_timer(etcd_slow_timeout, false, [this](int timer_id)
            {
                load_pgs();
            });
        }
        else
        {
            start_etcd_watcher();
        }
    });
}
