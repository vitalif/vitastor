// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#undef NDEBUG
#include <assert.h>
#include <stdexcept>
#include "cluster_client_impl.h"
#include "str_util.h"

inode_cache_t::~inode_cache_t()
{
    if (key_data)
    {
        free(key_data);
        key_data = NULL;
        op_enc = NULL;
    }
}

void cluster_client_t::vault_parse_config()
{
    vault_url = config["vault_url"].string_value();
    vault_client_cert = config["vault_client_cert"].string_value();
    if (vault_client_cert.empty())
        vault_client_cert = config["cert"].string_value();
    vault_client_key = config["vault_client_key"].string_value();
    if (vault_client_key.empty())
        vault_client_key = config["pkey"].string_value();
    vault_ca = config["vault_ca"].string_value();
    vault_secret_api_path = "/v1/secret/";
    if (config["vault_secret_api_path"].is_string())
        vault_secret_api_path = config["vault_secret_api_path"].string_value();
    vault_timeout_ms = config["vault_timeout_ms"].uint64_value();
    if (!vault_timeout_ms)
        vault_timeout_ms = 5000;
    vault_error_timeout_sec = config["vault_error_timeout_sec"].uint64_value();
    if (!vault_error_timeout_sec)
        vault_error_timeout_sec = 60;
    vault_refresh_leeway_sec = config["vault_refresh_leeway_sec"].uint64_value();
    if (!vault_refresh_leeway_sec)
        vault_refresh_leeway_sec = 60;
}

// FIXME: Rework client API by adding open/close and cache inode information in the "FD" (maybe)
void cluster_client_t::on_change_inode_hook(uint64_t inode, bool removed)
{
    std::vector<inode_t> children = { inode };
    for (size_t i = 0; i < children.size(); i++)
    {
        auto it = inode_cache_children.lower_bound(std::make_pair(children[i], (inode_t)0));
        while (it != inode_cache_children.end() && it->first == children[i])
        {
            children.push_back(it->second);
            it++;
        }
    }
    for (auto & inode: children)
    {
        auto it = inode_cache.find(inode);
        if (it != inode_cache.end())
        {
            auto icache = it->second;
            for (auto & parent: icache->chain)
            {
                inode_cache_children.erase(std::make_pair(parent, inode));
            }
            inode_cache.erase(it);
        }
    }
}

std::shared_ptr<inode_cache_t> cluster_client_t::inode_cache_get(inode_t ino)
{
    auto icache_it = inode_cache.find(ino);
    if (icache_it != inode_cache.end())
    {
        return icache_it->second;
    }
    // Fill inode cache
    auto ino_it = st_cli->inode_config.find(ino);
    if (ino_it == st_cli->inode_config.end())
    {
        inode_cache[ino] = NULL;
        return NULL;
    }
    auto pool_it = st_cli->pool_config.find(INODE_POOL(ino));
    if (pool_it == st_cli->pool_config.end())
    {
        inode_cache[ino] = NULL;
        return NULL;
    }
    auto & inode_cfg = ino_it->second;
    auto & pool_cfg = pool_it->second;
    std::shared_ptr<inode_cache_t> icache = std::make_shared<inode_cache_t>();
    icache->readonly = inode_cfg.readonly;
    icache->chain.push_back(ino);
    std::vector<inode_config_t*> chain_cfg;
    // FIXME: Allow unencrypted read & write when all chain is encrypted with the same key
    int enc_key_count = !inode_cfg.enc_key.empty() ? 1 : 0;
    if (inode_cfg.parent_id)
    {
        // Check for loops and cache the chain
        robin_hood::unordered_flat_set<inode_t> seen;
        seen.insert(ino);
        uint64_t parent_id = inode_cfg.parent_id;
        while (parent_id)
        {
            if (seen.find(parent_id) != seen.end())
            {
                icache->has_parent_loop = true;
                break;
            }
            seen.insert(parent_id);
            ino_it = st_cli->inode_config.find(parent_id);
            if (INODE_POOL(parent_id) == INODE_POOL(ino))
            {
                icache->chain.push_back(parent_id);
                if (ino_it == st_cli->inode_config.end())
                    chain_cfg.push_back(NULL);
                else
                {
                    chain_cfg.push_back(&ino_it->second);
                    if (!ino_it->second.enc_key.empty())
                        enc_key_count++;
                }
            }
            else if (!icache->other_pool_parent_id)
                icache->other_pool_parent_id = parent_id;
            if (ino_it == st_cli->inode_config.end())
                break;
            parent_id = ino_it->second.parent_id;
        }
    }
    // Check external keys and wait for loading, if required
    if (enc_key_count)
    {
        for (size_t i = 0; i <= chain_cfg.size(); i++)
        {
            inode_config_t *cfg = !i ? &inode_cfg : chain_cfg[i-1];
            if (cfg && cfg->enc_key.substr(0, strlen(VAULT_KEY_PREFIX)) == VAULT_KEY_PREFIX)
            {
                auto & ik = vault_keys[inode_cfg.enc_key];
                if (ik.key_state == VAULT_KEY_ERROR || vault_url.empty())
                {
                    icache->err_code = EPERM;
                    enc_key_count = 0;
                }
                else if (ik.key_state == VAULT_KEY_NOT_LOADED)
                {
                    ik.key_state = VAULT_KEY_LOADING;
                    vault_key_load_queue.push_back(inode_cfg.enc_key);
                    vault_load_keys();
                    icache->err_code = EAGAIN;
                    enc_key_count = 0;
                }
                else if (ik.key_state == VAULT_KEY_LOADING)
                {
                    icache->err_code = EAGAIN;
                    enc_key_count = 0;
                }
                else
                {
                    assert(ik.key_state == VAULT_KEY_LOADED);
                }
            }
        }
    }
    // Generate encryption key chain, if applicable
    if (enc_key_count)
    {
        uint8_t *key_data = (uint8_t*)malloc_or_die(
            AES_256_XTS_KEY_SIZE * enc_key_count +
            sizeof(uint8_t*) * icache->chain.size() +
            sizeof(osd_op_enc_t)
        );
        uint8_t **keys = (uint8_t**)(key_data + AES_256_XTS_KEY_SIZE * enc_key_count);
        osd_op_enc_t *enc = (osd_op_enc_t*)((uint8_t*)keys + sizeof(uint8_t*)*icache->chain.size());
        size_t key_pos = 0;
        for (size_t i = 0; i <= chain_cfg.size(); i++)
        {
            inode_config_t *cfg = !i ? &inode_cfg : chain_cfg[i-1];
            if (cfg && !cfg->enc_key.empty())
            {
                const auto & key = cfg->enc_key.substr(0, strlen(VAULT_KEY_PREFIX)) == VAULT_KEY_PREFIX
                    ? vault_keys.at(cfg->enc_key).key
                    : cfg->enc_key;
                assert(key_pos < AES_256_XTS_KEY_SIZE * enc_key_count);
                assert(key.size() == 2*AES_256_XTS_KEY_SIZE);
                keys[i] = key_data + key_pos;
                fromhexstr(key, AES_256_XTS_KEY_SIZE, key_data + key_pos);
                key_pos += AES_256_XTS_KEY_SIZE;
            }
            else
                keys[i] = NULL;
        }
        enc->key_chain = keys;
        enc->chain_size = icache->chain.size();
        enc->read_chain_bitmap_pos = pool_cfg.data_block_size/pool_cfg.bitmap_granularity/8;
        enc->bitmap_granularity = pool_cfg.bitmap_granularity;
        icache->key_data = key_data;
        icache->op_enc = enc;
    }
    inode_cache[ino] = icache;
    for (auto & parent: icache->chain)
    {
        if (parent != ino)
            inode_cache_children.insert(std::make_pair(parent, ino));
    }
    return icache;
}

void cluster_client_t::vault_parse_secret(const std::string & key_id, const std::string & err, json11::Json data)
{
    vault_loading = false;
    auto & k = vault_keys[key_id];
    if (err != "")
    {
        k.key_state = VAULT_KEY_ERROR;
        fprintf(stderr, "Vault %s%s%s request failed: %s\n", vault_url.c_str(),
            vault_secret_api_path.c_str(), key_id.c_str()+strlen(VAULT_KEY_PREFIX), err.c_str());
    }
    else
    {
        auto hexkey = data["data"]["key"].string_value();
        if (hexkey.empty() || !ishexstr(hexkey) || hexkey.size() != 2*AES_256_XTS_KEY_SIZE)
        {
            k.key_state = VAULT_KEY_ERROR;
            fprintf(stderr, "Vault /v1/secret/%s request failed: 'key' is empty or has invalid format\n", key_id.c_str());
        }
        else
        {
            k.key_state = VAULT_KEY_LOADED;
            k.key = hexkey;
        }
    }
    if (vault_key_load_queue.empty())
    {
        auto ops = std::move(key_wait_ops);
        for (cluster_op_t *op: ops)
            inode_cache.erase(op->inode);
        for (cluster_op_t *op: ops)
            execute_internal(op);
    }
    else
        vault_load_keys();
}
