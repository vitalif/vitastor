#include "osd.h"
#include "osd_http.h"

json11::Json osd_t::get_status()
{
    json11::Json::object st;
    st["state"] = "up";
    if (bind_address != "0.0.0.0")
        st["addresses"] = { bind_address };
    else
    {
        if (bind_addresses.size() == 0)
            bind_addresses = getifaddr_list();
        st["addresses"] = bind_addresses;
    }
    st["port"] = bind_port;
    st["blockstore_enabled"] = bs ? true : false;
    if (bs)
    {
        st["size"] = bs->get_block_count() * bs->get_block_size();
        st["free"] = bs->get_free_block_count() * bs->get_block_size();
    }
    json11::Json::object pg_status;
    for (auto & p: pgs)
    {
        auto & pg = p.second;
        json11::Json::object pg_st;
        json11::Json::array pg_state;
        for (int i = 0; i < pg_state_bit_count; i++)
            if (pg.state & pg_state_bits[i])
                pg_state.push_back(pg_state_names[i]);
        pg_st["state"] = pg_state;
        pg_st["object_count"] = pg.total_count;
        pg_st["clean_count"] = pg.clean_count;
        pg_st["misplaced_count"] = pg.misplaced_objects.size();
        pg_st["degraded_count"] = pg.degraded_objects.size();
        pg_st["incomplete_count"] = pg.incomplete_objects.size();
        pg_st["write_osd_set"] = pg.cur_set;
        pg_status[std::to_string(pg.pg_num)] = pg_st;
    }
    st["pgs"] = pg_status;
    json11::Json::object op_stats, subop_stats;
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        op_stats[osd_op_names[i]] = json11::Json::object {
            { "count", op_stat_count[0][i] },
            { "sum", op_stat_sum[0][i] },
        };
    }
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        subop_stats[osd_op_names[i]] = json11::Json::object {
            { "count", subop_stat_count[0][i] },
            { "sum", subop_stat_sum[0][i] },
        };
    }
    st["op_latency"] = op_stats;
    st["subop_latency"] = subop_stats;
    return st;
}

void osd_t::report_status()
{
    if (consul_host == "")
    {
        consul_host = consul_address;
        extract_port(consul_host);
    }
    std::string st = get_status().dump();
    std::string req = "PUT /v1/kv/"+consul_prefix+"/osd/"+std::to_string(osd_num)+" HTTP/1.1\r\n"+
        "Host: "+consul_host+"\r\n"+
        "Content-Length: "+std::to_string(st.size())+"\r\n"+
        "Connection: close\r\n"
        "\r\n"+st;
    http_request(consul_address, req, [this](int err, std::string res)
    {
        int pos = res.find("\r\n\r\n");
        if (pos >= 0)
        {
            res = res.substr(pos+4);
        }
        if (err != 0 || res != "true")
        {
            consul_failed_attempts++;
            printf("Error reporting state to Consul: code %d (%s), response text: %s\n", err, strerror(err), res.c_str());
            if (consul_failed_attempts > MAX_CONSUL_ATTEMPTS)
            {
                throw std::runtime_error("Cluster connection failed");
            }
            // Retry
            tfd->set_timer(CONSUL_RETRY_INTERVAL, false, [this](int timer_id)
            {
                report_status();
            });
        }
        else
        {
            consul_failed_attempts = 0;
        }
    });
}
