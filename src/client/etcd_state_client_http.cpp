// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>
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
        http_destroy(etcd_watch_ws);
        etcd_watch_ws = NULL;
    }
    if (keepalive_client)
    {
        http_destroy(keepalive_client);
        keepalive_client = NULL;
    }
    if (load_pgs_timer_id >= 0)
    {
        tfd->clear_timer(load_pgs_timer_id);
        load_pgs_timer_id = -1;
    }
    if (http_ctx)
    {
        http_context_destroy(http_ctx);
        http_ctx = NULL;
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

http_context_t *etcd_state_client_http_t::get_http_ctx()
{
    if (!http_ctx)
    {
        std::string error;
        http_ctx = http_context_init(tfd, etcd_client_cert, etcd_client_key, etcd_ca, true, error);
        if (!http_ctx)
        {
            fprintf(stderr, "Failed to initialize HTTP context: %s\n", error.c_str());
            exit(1);
        }
    }
    return http_ctx;
}

void etcd_state_client_http_t::etcd_call_oneshot(const std::string & etcd_url, const std::string & api, json11::Json payload,
    int timeout, std::function<void(std::string, json11::Json)> callback)
{
    auto http_cli = http_init(get_http_ctx());
    http_json_post(http_cli, etcd_url+api, payload, "", { .timeout = timeout }, [http_cli, callback](http_message_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        callback(err, data);
        http_destroy(http_cli);
    });
}

void etcd_state_client_http_t::etcd_call(const std::string & api, json11::Json payload, int timeout,
    int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    pick_next_etcd([=]()
    {
        etcd_call_selected(api, payload, timeout, retries, interval, callback);
    });
}

void etcd_state_client_http_t::etcd_call_selected(const std::string & api, json11::Json payload, int timeout,
    int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    const auto & url = selected_etcd_url;
    std::string req = payload.dump();
    req = "POST "+url.path+api+" HTTP/1.1\r\n"
        "Host: "+url.hostname+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: keep-alive\r\n"
        "Keep-Alive: timeout="+std::to_string(etcd_keepalive_timeout)+"\r\n"
        "\r\n"+req;
    retries--;
    auto cb = [this, api, payload, timeout, retries, interval, callback,
        cur_addr = url.addr](http_message_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        if (err != "")
        {
            if (cur_addr == selected_etcd_url.addr)
                selected_etcd_url = (http_url_t){};
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
        keepalive_client = http_init(get_http_ctx());
    http_request(keepalive_client, url.addr, req, { .timeout = timeout, .keepalive = true, .ssl = url.ssl }, cb);
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

void etcd_state_client_http_t::pick_next_etcd(std::function<void()> cb)
{
    if (!etcd_addresses.size() && !etcd_local.size())
    {
        fprintf(stderr, "etcd_address is missing in Vitastor configuration\n");
        exit(1);
    }
    if (selected_etcd_url.addr != "")
    {
        cb();
        return;
    }
    if (etcd_urls_to_try.size() != 0)
    {
        selected_etcd_url = std::move(etcd_urls_to_try[0]);
        etcd_urls_to_try.erase(etcd_urls_to_try.begin());
        cb();
        return;
    }
    on_resolve_queue.push_back(std::move(cb));
    if (on_resolve_queue.size() > 1)
    {
        // Already resolving
        return;
    }
    assert(!resolve_count);
    local_to_try = 0;
    for (auto & url: etcd_local_addr_urls)
    {
        // Prefer local IPs, if any
        etcd_urls_to_try.push_back(url);
        local_to_try++;
    }
    for (auto & url: etcd_nonlocal_addr_urls)
    {
        etcd_urls_to_try.push_back(url);
    }
    resolve_count++;
    for (auto & url: etcd_name_urls)
    {
        resolve_count++;
        http_resolve(get_http_ctx(), url.ssl, url.addr, [this, url](const std::string & error, const std::vector<std::string>& addresses)
        {
            if (error != "")
                fprintf(stderr, "Error resolving %s: %s\n", url.addr.c_str(), error.c_str());
            for (auto & addr: addresses)
            {
                auto url_copy = url;
                url_copy.addr = addr;
                if (local_ips.find(addr) != local_ips.end())
                {
                    etcd_urls_to_try.insert(etcd_urls_to_try.begin(), std::move(url_copy));
                    local_to_try++;
                }
                else
                    etcd_urls_to_try.push_back(std::move(url_copy));
            }
            resolve_count--;
            if (!resolve_count)
                pick_next_etcd_on_resolve();
        });
    }
    resolve_count--;
    if (!resolve_count)
    {
        pick_next_etcd_on_resolve();
    }
}

void etcd_state_client_http_t::pick_next_etcd_on_resolve()
{
    if (!etcd_urls_to_try.size())
    {
        fprintf(stderr, "None of etcd_address could be resolved\n");
        exit(1);
    }
    if (!rand_initialized)
    {
        timespec tv;
        clock_gettime(CLOCK_REALTIME, &tv);
        srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
        rand_initialized = true;
    }
    // Shuffle addresses
    for (size_t i = etcd_urls_to_try.size()-1; i > local_to_try; i--)
    {
        size_t j = local_to_try + lrand48() % (i - local_to_try);
        if (j != i)
            std::swap(etcd_urls_to_try[i], etcd_urls_to_try[j]);
    }
    selected_etcd_url = std::move(etcd_urls_to_try[0]);
    etcd_urls_to_try.erase(etcd_urls_to_try.begin());
    auto cbs = std::move(on_resolve_queue);
    for (auto cb: cbs)
    {
        cb();
    }
}

void etcd_state_client_http_t::start_etcd_watcher()
{
    pick_next_etcd([this]()
    {
        start_etcd_watcher_selected();
    });
}

void etcd_state_client_http_t::start_etcd_watcher_selected()
{
    const auto & url = selected_etcd_url;
    etcd_watches_initialised = 0;
    ws_alive = 1;
    if (this->log_level > 1)
    {
        fprintf(stderr, "Trying to connect to etcd websocket at %s%s%s (hostname %s), watch from revision %ju/%ju/%ju\n",
            url.ssl ? "https://" : "http://", url.addr.c_str(), url.path.c_str(), url.hostname.c_str(),
            etcd_watch_revision_config, etcd_watch_revision_osd, etcd_watch_revision_pg);
    }
    if (!etcd_watch_ws)
        etcd_watch_ws = http_init(get_http_ctx());
    else
        http_close(etcd_watch_ws);
    open_websocket(etcd_watch_ws, url.addr, url.hostname, url.path+"/watch", { .timeout = etcd_slow_timeout, .ssl = url.ssl },
        [this, cur_addr = url.addr](http_message_t *msg)
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
                    etcd_urls_to_try.clear();
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
            if (cur_addr == selected_etcd_url.addr)
                selected_etcd_url = (http_url_t){};
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
                    fprintf(stderr, "Websocket ping failed, disconnecting from etcd %s\n", selected_etcd_url.addr.c_str());
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
