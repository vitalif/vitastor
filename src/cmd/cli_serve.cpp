// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <signal.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include "cli.h"
#include "cluster_client.h"
#include "epoll_manager.h"
#include "http_client.h"
#include "str_util.h"
#include "json_util.h"
#include "addr_util.h"
#include "openapi.json.h"

struct cli_serve_conn_t
{
    int peer_fd = 0;
    std::string peer_addr;
    http_co_t *co = NULL;
    cli_tool_t *p = NULL;
    cli_result_t result;
    bool keepalive = false;
    bool closed = false;
    timespec request_time;
    std::string request_method;
    std::string request_path;
    std::string request_body;
    std::string response_type;
    std::function<bool(cli_result_t &)> action_cb;
};

struct cli_serve_path_t
{
    std::string cmd;
    bool allow_get;
    bool allow_client;
};

// Serve vitastor-cli commands over HTTP in JSON format
struct cli_serve_t
{
    std::map<std::string, cli_serve_path_t> cmd_paths = {
        {"data/delete",   {"rm-data",     false, false}},
        {"data/describe", {"describe",    true,  false}},
        {"data/fix",      {"fix",         false, false}},
        {"data/merge",    {"merge-data",  false, false}},
        {"image/create",  {"create",      false, true}},
        {"image/delete",  {"rm",          false, true}},
        {"image/flatten", {"flatten",     false, true}},
        {"image/list",    {"ls",          true,  true}},
        {"image/modify",  {"modify",      false, true}},
        {"osd/alloc",     {"alloc-osd",   false, false}},
        {"osd/delete",    {"rm-osd",      false, false}},
        {"osd/list",      {"ls-osd",      true,  false}},
        {"osd/modify",    {"modify-osd",  false, false}},
        {"pg/list",       {"ls-pgs",      true,  false}},
        {"pool/create",   {"create-pool", false, false}},
        {"pool/delete",   {"rm-pool",     false, false}},
        {"pool/list",     {"pools",       true,  false}},
        {"pool/modify",   {"modify-pool", false, false}},
        {"user/delete",   {"remove-user", false, false}},
        {"user/list",     {"ls-user",     false, false}},
        {"user/modify",   {"modify-user", false, false}},
        {"status",        {"status",      true,  false}},
    };

    cli_tool_t *parent = NULL;
    json11::Json options;
    cli_result_t result;

    bool log_body = false;
    bool stop = false;
    std::vector<std::string> bind_addresses;
    int port = 0;
    int listen_backlog = 0;
    bool ssl = false;
    bool use_perms = false;
    std::vector<int> listen_fds;
    http_context_t *http_ctx = NULL;
    std::set<cli_serve_conn_t*> connections;

    int state = 0;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        else if (state == 100)
            return;
        if (options["bind_address"].is_string())
            bind_addresses = explode(" ", options["bind_address"].string_value(), true);
        else
            bind_addresses.push_back("127.0.0.1");
        port = options["port"].uint64_value();
        if (!port)
            port = 8080;
        else if (port < 0 || port > 65535)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "HTTP port can't be larger than 65536" };
            state = 100;
            return;
        }
        listen_backlog = options["listen_backlog"].uint64_value();
        if (!listen_backlog)
            listen_backlog = 128;
        use_perms = json_is_true(parent->cli->config["use_perms"]);
        {
            std::string api_cert = (parent->cli->config.find("api_cert") != parent->cli->config.end()
                ? parent->cli->config["api_cert"].string_value() : "");
            std::string api_pkey = (parent->cli->config.find("api_pkey") != parent->cli->config.end()
                ? parent->cli->config["api_pkey"].string_value() : "");
            std::string client_ca = (parent->cli->config.find("client_ca") != parent->cli->config.end()
                ? parent->cli->config["client_ca"].string_value() : "");
            std::string admin_ca = (parent->cli->config.find("admin_ca") != parent->cli->config.end()
                ? parent->cli->config["admin_ca"].string_value() : "");
            if (api_cert != "" || api_pkey != "" || client_ca != "" || admin_ca != "")
            {
                ssl = true;
                if (api_cert == "" || api_pkey == "")
                {
                    result = (cli_result_t){ .err = EINVAL, .text = "api_cert and api_pkey are required to serve HTTPS" };
                    state = 100;
                    return;
                }
            }
            std::string error;
            http_ctx = http_context_init(parent->epmgr->tfd, api_cert, api_pkey, client_ca, client_ca != "", error);
            if (error != "")
            {
                result = (cli_result_t){ .err = EINVAL, .text = error };
                state = 100;
                return;
            }
            if (client_ca != "" && admin_ca != "")
            {
                bool ok = http_context_add_ca(http_ctx, admin_ca);
                if (!ok)
                {
                    http_context_destroy(http_ctx);
                    result = (cli_result_t){ .err = EINVAL, .text = "Failed to load admin_ca" };
                    state = 100;
                    return;
                }
            }
            if (use_perms && client_ca == "")
            {
                result = (cli_result_t){ .err = EINVAL, .text = "use_perms requires client_ca" };
                state = 100;
                return;
            }
        }
        for (auto & bind_address: bind_addresses)
        {
            int listen_fd = create_and_bind_socket(bind_address, port, listen_backlog, NULL);
            fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);
            parent->epmgr->set_fd_handler(listen_fd, false, [this](int fd, int events)
            {
                accept_connections(fd);
            });
            listen_fds.push_back(listen_fd);
        }
    resume_1:
        if (!stop)
        {
            state = 1;
            return;
        }
        for (auto conn: connections)
        {
            http_close(conn->co);
        }
    resume_2:
        // Wait for all connections to finish
        if (connections.size() > 0)
        {
            state = 2;
            return;
        }
        if (http_ctx)
        {
            http_context_destroy(http_ctx);
            http_ctx = NULL;
        }
        state = 100;
    }

    void accept_connections(int listen_fd)
    {
        sockaddr_storage addr;
        socklen_t peer_addr_size = sizeof(addr);
        int peer_fd;
        while ((peer_fd = accept(listen_fd, (sockaddr*)&addr, &peer_addr_size)) >= 0)
        {
            auto peer_addr_str = addr_to_string(addr);
            assert(peer_fd != 0);
            timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            printf("[%s.%03ju] New connection %d from %s\n", format_datetime(ts.tv_sec).c_str(), (uint64_t)ts.tv_nsec/1000000,
                peer_fd, peer_addr_str.c_str());
            fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
            int one = 1;
            setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
            cli_serve_conn_t *conn = new cli_serve_conn_t;
            conn->peer_fd = peer_fd;
            conn->peer_addr = peer_addr_str;
            conn->co = http_init(http_ctx);
            http_serve(conn->co, peer_fd, (http_options_t){ .ssl = ssl }, [this, conn](http_message_t *msg)
            {
                process_request(conn, msg);
            });
            connections.insert(conn);
            // Try to accept next connection
            peer_addr_size = sizeof(addr);
        }
        if (peer_fd == -1 && errno != EAGAIN)
        {
            throw std::runtime_error(std::string("accept: ") + strerror(errno));
        }
    }

    int map_to_http(int err, std::string *text)
    {
        int code = 0;
        if (err == EINVAL)
        {
            code = 400;
            if (text)
                *text = "Bad Request";
        }
        else if (err == EACCES)
        {
            code = 403;
            if (text)
                *text = "Forbidden";
        }
        else if (err == EOPNOTSUPP)
        {
            code = 404;
            if (text)
                *text = "Not Found";
        }
        else if (err == ENOSYS)
        {
            code = 405;
            if (text)
                *text = "Method Not Allowed";
        }
        else if (err == EAGAIN)
        {
            code = 409;
            if (text)
                *text = "Update Conflict";
        }
        else if (err == ENOTEMPTY || err == EEXIST || err == ENOENT || err == EBUSY)
        {
            code = 412;
            if (text)
                *text = "Precondition Failed";
        }
        else /*if (err == EIO || err == EBADF)*/
        {
            code = 500;
            if (text)
                *text = "Internal Server Error";
        }
        return code;
    }

    std::string cli_http_response(cli_serve_conn_t *conn)
    {
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        int code = 200;
        std::string response;
        if (conn->result.err)
        {
            std::string status_line;
            code = map_to_http(conn->result.err, &status_line);
            response = "HTTP/1.1 "+std::to_string(code)+" "+status_line+"\r\n";
        }
        else
        {
            response = "HTTP/1.1 200 OK\r\n";
        }
        response += (conn->keepalive
            ? "Connection: keep-alive\r\n"
            : "Connection: close\r\n");
        std::string body;
        if (!conn->result.data.is_null())
        {
            response += "Content-Type: application/json\r\n";
            body = conn->result.data.dump();
        }
        else
        {
            if (!conn->response_type.empty())
                response += "Content-Type: "+conn->response_type+"\r\n";
            else
                response += "Content-Type: text/plain; charset=utf-8\r\n";
            body = conn->result.text;
        }
        response += "Content-Length: "+std::to_string(body.size())+"\r\n\r\n";
        response += body;
        if (conn->request_method.find("\n") != std::string::npos)
            conn->request_method = str_replace(conn->request_method, "\n", "%0a");
        if (conn->request_method.find(" ") != std::string::npos)
            conn->request_method = str_replace(conn->request_method, " ", "%20");
        if (conn->request_path.find("\n") != std::string::npos)
            conn->request_path = str_replace(conn->request_path, "\n", "%0a");
        if (conn->request_path.find(" ") != std::string::npos)
            conn->request_path = str_replace(conn->request_path, " ", "%20");
        uint64_t response_time = (now.tv_sec-conn->request_time.tv_sec)*1000 + (now.tv_nsec-conn->request_time.tv_nsec)/1000000;
        printf("[%s.%03ju] %s %s %s %d %.03f sec\n",
            format_datetime(now.tv_sec).c_str(), (uint64_t)now.tv_nsec/1000000,
            conn->peer_addr.c_str(), conn->request_method.c_str(), conn->request_path.c_str(), code,
            response_time/1000.0);
        if (log_body)
        {
            if (conn->request_body.find("\n") != std::string::npos)
                conn->request_body = str_replace(conn->request_body, "\n", " ");
            if (conn->request_body.size())
                printf("  %s\n", conn->request_body.c_str());
            printf("  %s\n", body.c_str());
        }
        return response;
    }

    void process_request(cli_serve_conn_t *conn, http_message_t *msg)
    {
        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        if (!msg->error.empty())
        {
            // connection is closed
            fprintf(stderr, "[%s.%03ju] Connection %d closed: %s\n", format_datetime(ts.tv_sec).c_str(),
                (uint64_t)ts.tv_nsec/1000000, conn->peer_fd, msg->error.c_str());
            if (conn->p)
                conn->closed = true;
            else
            {
                connections.erase(conn);
                http_destroy(conn->co);
                delete conn;
            }
            return;
        }
        conn->keepalive = msg->headers.find("connection") != msg->headers.end() &&
            msg->headers.at("connection") == "keep-alive";
        conn->p = new cli_tool_t;
        conn->p->iodepth = parent->iodepth;
        conn->p->parallel_osds = parent->parallel_osds;
        conn->p->json_output = true;
        conn->p->ringloop = parent->ringloop;
        conn->p->epmgr = parent->epmgr;
        conn->p->cli = parent->cli;
        conn->p->is_command_line = false;
        // Parse request
        auto req_line = explode(" ", msg->status_line, true);
        if (req_line.empty())
        {
            conn->result = { .err = EINVAL, .text = "Invalid request line" };
            http_reply(conn->co, cli_http_response(conn));
            delete conn->p;
            conn->p = NULL;
            return;
        }
        if (req_line.size() < 2)
        {
            // Checked if req_line is empty, so size must be 1 now
            req_line.push_back("-");
        }
        conn->request_time = ts;
        conn->request_method = std::move(req_line[0]);
        conn->request_path = std::move(req_line[1]);
        conn->request_body = std::move(msg->body);
        conn->response_type = "";
        if (use_perms)
        {
            conn->p->user = parent->cli->st_cli->get_user(msg->tls_cn);
            conn->p->is_admin = msg->tls_ca_idx >= 1;
        }
        // Don't insert a new value
        auto ctype_it = msg->headers.find("content-type");
        std::string ctype = (ctype_it != msg->headers.end()) ? ctype_it->second : "";
        if (conn->request_method != "GET" && conn->request_method != "POST")
        {
            conn->result = { .err = ENOSYS, .text = "Unsupported request method "+conn->request_method };
        }
        else if (ctype != (conn->request_method == "GET" ? "" : "application/json"))
        {
            conn->result = { .err = EINVAL, .text = "Unsupported Content-Type: "+ctype+" for "+conn->request_method+" requests" };
        }
        else
        {
            auto uri = explode("?", conn->request_path, true);
            uri[0] = trim(uri[0], "/");
            auto cmd_it = cmd_paths.find(uri[0]);
            if (uri[0] == "")
            {
                std::string text = "Supported APIs:\n\n- GET /openapi\n";
                for (auto & pp: cmd_paths)
                {
                    text += (pp.second.allow_get ? "- GET" : "- POST") + (" /" + pp.first) + "\n";
                }
                conn->result = { .text = text };
            }
            else if (uri[0] == "openapi")
            {
                conn->response_type = "application/json";
                conn->result = { .text = openapi_description };
                if (use_perms)
                {
                    // Filter available paths by privileges
                    if (!conn->p->is_admin)
                    {
                        std::string error;
                        auto openapi = json11::Json::parse(openapi_description, error).object_items();
                        json11::Json::object paths;
                        for (auto & kv: openapi["paths"].object_items())
                        {
                            auto cmd_it = cmd_paths.find(kv.first.substr(1));
                            if (cmd_it != cmd_paths.end() && cmd_it->second.allow_client)
                            {
                                paths[kv.first] = kv.second;
                            }
                        }
                        openapi["paths"] = paths;
                        conn->response_type = "application/json";
                        conn->result = { .text = json11::Json(openapi).dump() };
                    }
                    else if (!conn->p->user)
                    {
                        conn->response_type = "";
                        conn->result = { .err = EACCES, .text = "Access denied" };
                    }
                }
            }
            else if (cmd_it == cmd_paths.end())
            {
                conn->result = { .err = EOPNOTSUPP, .text = "unknown command: "+uri[0] };
            }
            else if (conn->request_method == "GET" && !cmd_it->second.allow_get)
            {
                conn->result = { .err = ENOSYS, .text = "method /"+uri[0]+" only allows POST requests" };
            }
            else if (use_perms && !conn->p->is_admin && !cmd_it->second.allow_client)
            {
                conn->result = { .err = EACCES, .text = "Access denied" };
            }
            else
            {
                std::string error;
                json11::Json::object cfg;
                if (conn->request_method == "POST")
                {
                    cfg = json11::Json::parse(conn->request_body, error).object_items();
                }
                else
                {
                    // Parse URI
                    cfg = parse_uri_params(uri.size() > 1 ? uri[1] : "");
                }
                if (error != "")
                {
                    conn->result = { .err = EINVAL, .text = "Invalid JSON in body: "+error };
                }
                else
                {
                    cfg["command"] = json11::Json::array{cmd_it->second.cmd};
                    conn->p->parse_api_opts(cfg);
                    conn->action_cb = conn->p->start(cfg, conn->result);
                }
            }
        }
        if (!conn->action_cb)
        {
            http_reply(conn->co, cli_http_response(conn));
            delete conn->p;
            conn->p = NULL;
            return;
        }
        conn->p->loop_and_wait(conn->action_cb, [this, conn](const cli_result_t & r)
        {
            conn->result = r;
            conn->action_cb = NULL;
            delete conn->p;
            conn->p = NULL;
            if (!conn->closed)
                http_reply(conn->co, cli_http_response(conn));
            else
            {
                connections.erase(conn);
                http_destroy(conn->co);
                delete conn;
            }
        });
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_serve(json11::Json cfg)
{
    auto server = new cli_serve_t();
    server->parent = this;
    server->options = cfg;
    return [server](cli_result_t & result)
    {
        server->loop();
        if (server->is_done())
        {
            result = server->result;
            delete server;
            return true;
        }
        return false;
    };
}
