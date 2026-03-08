// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "cluster_client.h"
#include "cluster_client_impl.h"
#include "etcd_state_client_http.h"
#include "http_client.h"

cluster_client_t* cluster_client_t::create(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json config)
{
    auto st_cli = new etcd_state_client_http_t(tfd);
    return new cluster_client_t(ringloop, tfd, config, std::unique_ptr<etcd_state_client_t>(st_cli));
}

bool cluster_client_t::vault_check_token()
{
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    if (!vault_token_expire.tv_sec || vault_token_expire.tv_sec < now.tv_sec)
    {
        vault_loading = true;
        http_json_post(
            vault_http_cli, vault_url+"/v1/auth/cert/login", json11::Json::object{}, "",
            (http_options_t){ .timeout = (int)vault_timeout_ms, .keepalive = true },
            [this](http_message_t *response)
            {
                clock_gettime(CLOCK_REALTIME, &vault_token_expire);
                vault_loading = false;
                std::string err;
                json11::Json data;
                response->parse_json_response(err, data);
                if (err != "")
                {
                    vault_token_expire.tv_sec += vault_error_timeout_sec;
                    fprintf(stderr, "Vault request failed: %s\n", err.c_str());
                }
                else
                {
                    uint64_t ttl = data["auth"]["lease_duration"].uint64_value();
                    vault_token = data["auth"]["client_token"].string_value();
                    if (vault_token.empty() || !ttl)
                    {
                        vault_token_expire.tv_sec += vault_error_timeout_sec;
                        fprintf(stderr, "No token or lease_duration in Vault response: %s\n", data.dump().c_str());
                    }
                    else
                    {
                        if (ttl < vault_refresh_leeway_sec)
                            vault_token_expire.tv_sec += ttl/2;
                        else
                            vault_token_expire.tv_sec += ttl - vault_refresh_leeway_sec;
                    }
                }
                vault_load_keys();
            }
        );
        return false;
    }
    if (vault_token.empty())
    {
        // Auth error happened, mark all loads as failed
        for (auto & key_id: vault_key_load_queue)
        {
            auto & k = vault_keys[key_id];
            k.key_state = VAULT_KEY_ERROR;
        }
        vault_key_load_queue.clear();
        auto ops = std::move(key_wait_ops);
        for (cluster_op_t *op: ops)
            inode_cache.erase(op->inode);
        for (cluster_op_t *op: ops)
            execute_internal(op);
        return false;
    }
    return true;
}

void cluster_client_t::vault_destroy()
{
    if (vault_http_ctx)
    {
        http_destroy(vault_http_cli);
        http_context_destroy(vault_http_ctx);
        vault_http_cli = NULL;
        vault_http_ctx = NULL;
    }
}

void cluster_client_t::vault_load_keys()
{
    if (vault_loading || !vault_key_load_queue.size())
    {
        return;
    }
    if (!vault_http_ctx)
    {
        std::string error;
        vault_http_ctx = http_context_init(tfd, vault_client_cert, vault_client_key, vault_ca, true, error);
        if (!vault_http_ctx)
        {
            fprintf(stderr, "Failed to initialize HTTP context for Vault: %s\n", error.c_str());
            exit(1);
        }
        vault_http_cli = http_init(vault_http_ctx);
    }
    if (!vault_check_token())
    {
        return;
    }
    std::string key_id = vault_key_load_queue[0];
    vault_key_load_queue.erase(vault_key_load_queue.begin());
    vault_loading = true;
    http_get(
        vault_http_cli, vault_url+vault_secret_api_path+key_id.substr(strlen(VAULT_KEY_PREFIX)), "X-Vault-Token: "+vault_token+"\r\n",
        (http_options_t){ .timeout = (int)vault_timeout_ms, .keepalive = true },
        [this, key_id](http_message_t *response)
        {
            vault_loading = false;
            std::string err;
            json11::Json data;
            response->parse_json_response(err, data);
            vault_parse_secret(key_id, err, data);
        }
    );
}
