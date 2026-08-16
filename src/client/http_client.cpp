// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <arpa/inet.h>

#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>

#include <stdexcept>

#ifdef WITH_OPENSSL
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include "openssl_util.h"
#endif

// libc-ares
#include <ares.h>

#include "addr_util.h"
#include "str_util.h"
#include "json_util.h"
#include "json11/json11.hpp"
#include "http_client.h"
#include "timerfd_manager.h"

#define READ_BUFFER_SIZE 9000

static std::string ws_format_frame(int type, uint64_t size);
static bool ws_parse_frame(std::string & buf, uint8_t & type, std::string & res);
static void parse_http_headers(std::string & res, http_message_t *parsed, bool is_request);

struct http_context_t
{
    timerfd_manager_t *tfd = NULL;
    ares_channel ares = NULL;

    std::string ssl_cert;
    std::string ssl_key;
    std::string ssl_ca;

#ifdef WITH_OPENSSL
    SSL_CTX *ssl_ctx = NULL;
    std::string ssl_cn;
    std::vector<X509*> ssl_ca_certs;
#endif

    ~http_context_t()
    {
        ares_destroy(ares);
        ares = NULL;
#ifdef WITH_OPENSSL
        if (ssl_ctx)
        {
            SSL_CTX_free(ssl_ctx);
            ssl_ctx = NULL;
        }
        for (X509 *ca: ssl_ca_certs)
        {
            X509_free(ca);
        }
        ssl_ca_certs.clear();
#endif
    }
};

struct http_call_t
{
    std::string host;
    std::string request;
    http_options_t options;
    std::function<void(http_message_t *)> cb;
};

struct http_co_t
{
    http_context_t *ctx = NULL;
#ifdef WITH_OPENSSL
    SSL *ssl_cli = NULL;
    BIO *ssl_bio = NULL;
#endif

    timerfd_manager_t *tfd = NULL;

    std::function<void(http_message_t*)> response_callback;

    int request_timeout = 0;
    bool ssl = false;
    std::string host;
    std::string host_port;
    std::string request;
    std::string ws_outbox;
    std::string response;
    bool want_streaming;
    bool keepalive;

    std::vector<http_call_t> keepalive_queue;

    int state = 0;
    std::string connected_host;
    int peer_fd = -1;
    int timeout_id = -1;
    int epoll_events = 0;
    int sent = 0;
    std::vector<uint8_t> rbuf;
    iovec read_iov, send_iov;
    msghdr read_msg = { 0 }, send_msg = { 0 };
    http_message_t parsed;
    uint64_t target_response_size = 0;

    int onstack = 0;
    bool ended = false;

    ~http_co_t();
    inline void stackin() { onstack++; }
    inline void stackout() { onstack--; if (!onstack && ended) end(); }
    inline void end() { ended = true; if (!onstack) { delete this; } }
    void run_cb_and_clear();
    void resolve_and_connect();
    void start_connection(sockaddr *addr, size_t addr_size);
    void start_ws_connection();
    void close_connection();
    void next_request();
    void handle_events();
    void handle_connect_result();
    void submit_read(bool check_timeout);
    void submit_send();
    bool handle_read();
#ifdef WITH_OPENSSL
    bool do_ssl_handshake(bool init_send);
    void on_ssl_error(int res);
#endif
    void post_message(uint8_t type, const std::string & msg);
    void reply(const std::string & msg);
    void send_request(const std::string & host, const std::string & request,
        const http_options_t & options, std::function<void(http_message_t *response)> response_callback);

    void ares_addrinfo_cb(int status, int timeouts, struct ares_addrinfo *result);
};

#define HTTP_CO_CLOSED 0
#define HTTP_CO_RESOLVING 1
#define HTTP_CO_CONNECTING 2
#define HTTP_CO_SENDING_REQUEST 3
#define HTTP_CO_REQUEST_SENT 4
#define HTTP_CO_HEADERS_RECEIVED 5
#define HTTP_CO_WEBSOCKET 6
#define HTTP_CO_CHUNKED 7
#define HTTP_CO_KEEPALIVE 8
#define HTTP_CO_SERVER 9
#define HTTP_CO_REQ_HDR_RECEIVED 10
#define HTTP_CO_REQUEST_RECEIVED 11

#define DEFAULT_TIMEOUT 5000

void http_ares_cb(void *data, ares_socket_t socket_fd, int readable, int writable)
{
    http_context_t *ctx = (http_context_t *)data;
    ctx->tfd->set_fd_handler(socket_fd, writable, [ctx](int fd, int epoll_events)
    {
        ares_process_fd(ctx->ares, (epoll_events & (EPOLLIN|EPOLLRDHUP)) ? fd : 0, (epoll_events & EPOLLOUT) ? fd : 0);
    });
}

http_context_t* http_context_init(timerfd_manager_t *tfd, const std::string & ssl_cert, const std::string & ssl_key,
    const std::string & ssl_ca, bool verify_peer, std::string & error)
{
    http_context_t *ctx = new http_context_t;
    ctx->tfd = tfd;
    ares_options opts = {
        .sock_state_cb = http_ares_cb,
        .sock_state_cb_data = ctx,
    };
    ares_init_options(&ctx->ares, &opts, ARES_OPT_SOCK_STATE_CB);
#ifdef WITH_OPENSSL
    SSL_CTX *ssl_ctx = SSL_CTX_new(TLS_method());
    ctx->ssl_cert = ssl_cert;
    ctx->ssl_key = ssl_key;
    ctx->ssl_ctx = ssl_ctx;
    ctx->ssl_cn = "";
    if (!ssl_ctx)
        goto init_err;
    SSL_CTX_set_verify(ssl_ctx, verify_peer ? SSL_VERIFY_PEER : SSL_VERIFY_NONE, NULL);
    if (!SSL_CTX_set_min_proto_version(ssl_ctx, TLS1_2_VERSION))
        goto init_err;
    if (ssl_ca != "")
    {
        X509 *ca = openssl_load_cert(ssl_ca);
        if (!ca)
            goto init_err;
        ctx->ssl_ca_certs.push_back(ca);
        X509_STORE_add_cert(SSL_CTX_get_cert_store(ssl_ctx), ca);
    }
    if (ssl_cert != "" && ssl_key != "" &&
        (!openssl_ctx_use_cert(ssl_ctx, ssl_cert, ctx->ssl_cn) ||
        !openssl_ctx_use_key(ssl_ctx, ssl_key)))
        goto init_err;
#endif
    return ctx;
init_err:
    error = std::string("openssl initialization failed: ")+ERR_error_string(ERR_get_error(), NULL);
    delete ctx;
    return NULL;
}

bool http_context_add_ca(http_context_t *ctx, const std::string & add_ca)
{
    if (!ctx->ssl_ctx || !ctx->ssl_ca_certs.size())
        return false;
    X509 *ca = openssl_load_cert(add_ca);
    if (!ca)
        return false;
    ctx->ssl_ca_certs.push_back(ca);
    X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx->ssl_ctx), ca);
    return true;
}

std::string http_context_get_ssl_cn(http_context_t *ctx)
{
    return ctx->ssl_cn;
}

struct http_ctx_resolve_t
{
    std::function<void(const std::string & error, const std::vector<std::string> & addrs)> cb;
};

void http_resolve_ares_cb(void *data, int status, int timeouts, struct ares_addrinfo *result)
{
    http_ctx_resolve_t *obj = (http_ctx_resolve_t*)data;
    if (status != ARES_SUCCESS)
    {
        obj->cb(ares_strerror(status), {});
        delete obj;
        return;
    }
    std::vector<std::string> addrs;
    for (auto node = result->nodes; node; node = node->ai_next)
    {
        sockaddr_storage ss;
        memset(&ss, 0, sizeof(ss));
        memcpy(&ss, node->ai_addr, node->ai_addrlen);
        addrs.push_back(addr_to_string(ss));
    }
    obj->cb("", addrs);
    delete obj;
}

void http_resolve(http_context_t *ctx, bool ssl, std::string host,
    std::function<void(const std::string & error, const std::vector<std::string> & addrs)> cb)
{
    auto obj = new http_ctx_resolve_t();
    obj->cb = std::move(cb);
    ares_addrinfo_hints hints = { .ai_flags = ARES_AI_NOSORT|ARES_AI_NUMERICSERV };
    auto pos = host.rfind(':');
    if (pos != std::string::npos)
        host[pos] = 0;
    ares_getaddrinfo(ctx->ares, host.c_str(),
        pos != std::string::npos ? host.c_str()+pos+1 : (ssl ? "443" : "80"), &hints, http_resolve_ares_cb, obj);
}

void http_context_destroy(http_context_t *ctx)
{
    delete ctx;
}

http_co_t *http_init(http_context_t *ctx)
{
    http_co_t *handler = new http_co_t();
    handler->tfd = ctx->tfd;
    handler->state = HTTP_CO_CLOSED;
    handler->ctx = ctx;
    return handler;
}

void open_websocket(http_co_t *handler, const std::string & addr, const std::string & hostname, const std::string & path,
    const http_options_t & options, std::function<void(http_message_t *msg)> response_callback)
{
    if (handler->state == HTTP_CO_KEEPALIVE && (handler->connected_host != addr || handler->ssl != options.ssl))
        handler->close_connection();
    if (handler->state != HTTP_CO_KEEPALIVE && handler->state != HTTP_CO_CLOSED)
        throw std::runtime_error("Attempt to open websocket on a keepalive stream");
    std::string request = "GET "+path+" HTTP/1.1\r\n"
        "Host: "+hostname+"\r\n"
        "Upgrade: websocket\r\n"
        "Connection: upgrade\r\n"
        "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    handler->host = addr;
    handler->host_port = "";
    handler->request_timeout = options.timeout < 0 ? -1 : (options.timeout == 0 ? DEFAULT_TIMEOUT : options.timeout);
    handler->want_streaming = false;
    handler->keepalive = false;
    handler->ssl = options.ssl;
    handler->request = request;
    handler->response_callback = response_callback;
    handler->ws_outbox = "";
    handler->response = "";
    handler->sent = 0;
    handler->parsed = {};
    handler->start_ws_connection();
}

void http_request(http_co_t *handler, const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(http_message_t *response)> response_callback)
{
    handler->send_request(host, request, options, response_callback);
}

void http_get(http_co_t *handler, const std::string & url, const std::string & headers,
    const http_options_t & options, std::function<void(http_message_t *response)> response_callback)
{
    std::string path;
    auto ssl = url.substr(0, 8) == "https://";
    auto host = url.substr(ssl ? 8 : 7);
    auto pos = host.find('/');
    if (pos != std::string::npos)
    {
        path = host.substr(pos);
        host = host.substr(0, pos);
    }
    std::string req = "GET "+path+" HTTP/1.1\r\n"
        "Host: "+host+"\r\n"
        "Connection: "+(options.keepalive ? "keep-alive" : "close")+"\r\n"+
        headers+"\r\n";
    handler->send_request(host, req, { .timeout = options.timeout, .keepalive = options.keepalive, .ssl = ssl }, response_callback);
}

void http_json_post(http_co_t *handler, const std::string & url, json11::Json body, const std::string & headers,
    const http_options_t & options, std::function<void(http_message_t *response)> response_callback)
{
    std::string path;
    auto ssl = url.substr(0, 8) == "https://";
    auto host = url.substr(ssl ? 8 : 7);
    auto pos = host.find('/');
    if (pos != std::string::npos)
    {
        path = host.substr(pos);
        host = host.substr(0, pos);
    }
    std::string req = body.dump();
    req = "POST "+path+" HTTP/1.1\r\n"
        "Host: "+host+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: "+(options.keepalive ? "keep-alive" : "close")+"\r\n"+
        headers+
        "\r\n"+req;
    handler->send_request(host, req, { .timeout = options.timeout, .keepalive = options.keepalive, .ssl = ssl }, response_callback);
}

void http_serve(http_co_t *handler, int peer_fd, const http_options_t & options, std::function<void(http_message_t *msg)> request_callback)
{
    if (handler->state != HTTP_CO_SERVER || handler->peer_fd != peer_fd)
        handler->close_connection();
    handler->host = "";
    handler->host_port = "";
    handler->request_timeout = options.timeout < 0 ? -1 : (options.timeout == 0 ? DEFAULT_TIMEOUT : options.timeout);
    handler->want_streaming = false;
    handler->keepalive = false;
    handler->ssl = options.ssl;
    handler->request = "";
    handler->response_callback = request_callback;
    handler->ws_outbox = "";
    handler->response = "";
    handler->sent = 0;
    handler->parsed = {};
    handler->peer_fd = peer_fd;
    handler->state = HTTP_CO_SERVER;
    handler->tfd->set_fd_handler(peer_fd, false, [handler](int peer_fd, int epoll_events)
    {
        handler->epoll_events |= epoll_events;
        handler->handle_events();
    });
}

void http_co_t::run_cb_and_clear()
{
    parsed.eof = true;
    std::function<void(http_message_t*)> cb;
    cb.swap(response_callback);
    // Call callback after clearing it because otherwise we may hit reenterability problems
    if (cb != NULL)
        cb(&parsed);
    next_request();
}

void http_co_t::send_request(const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(http_message_t *response)> response_callback)
{
    stackin();
    if (state == HTTP_CO_WEBSOCKET)
    {
        stackout();
        throw std::runtime_error("Attempt to send HTTP request into a websocket or chunked stream");
    }
    else if (state != HTTP_CO_KEEPALIVE && state != HTTP_CO_CLOSED)
    {
        keepalive_queue.emplace_back((http_call_t){ host, request, options, std::move(response_callback) });
        stackout();
        return;
    }
    if (state == HTTP_CO_KEEPALIVE && (connected_host != host || ssl != options.ssl))
    {
        close_connection();
    }
    this->request_timeout = options.timeout < 0 ? 0 : (options.timeout == 0 ? DEFAULT_TIMEOUT : options.timeout);
    this->want_streaming = options.want_streaming;
    this->keepalive = options.keepalive;
    this->ssl = options.ssl;
    this->host = host;
    this->host_port = "";
    this->request = request;
    this->response = "";
    this->sent = 0;
    this->response_callback = response_callback;
    this->parsed = {};
    if (state == HTTP_CO_KEEPALIVE)
    {
        state = HTTP_CO_SENDING_REQUEST;
        submit_send();
    }
    else
    {
        resolve_and_connect();
    }
    // Do it _after_ state assignment because set_timer() can actually trigger
    // other timers and requests (reenterability is our friend)
    if (request_timeout > 0)
    {
        timeout_id = tfd->set_timer(request_timeout, false, [this](int timer_id)
        {
            stackin();
            if (state == HTTP_CO_REQUEST_SENT)
            {
                // In case of high CPU load, we may not handle etcd responses in time
                // For this case, first check the socket and only then terminate request with the timeout
                submit_read(true);
            }
            else
            {
                close_connection();
                parsed = { .error = "HTTP request timed out", .status_code = ETIMEDOUT };
                run_cb_and_clear();
            }
            stackout();
        });
    }
    stackout();
}

void http_post_message(http_co_t *handler, uint8_t type, const std::string & msg)
{
    handler->post_message(type, msg);
}

void http_reply(http_co_t *handler, const std::string & reply)
{
    handler->reply(reply);
}

void http_co_t::post_message(uint8_t type, const std::string & msg)
{
    stackin();
    if (state == HTTP_CO_WEBSOCKET)
    {
        request += ws_format_frame(type, msg.size());
        request += msg;
        submit_send();
    }
    else if (state == HTTP_CO_KEEPALIVE || state == HTTP_CO_CHUNKED ||
        state == HTTP_CO_SERVER || state == HTTP_CO_REQ_HDR_RECEIVED || state == HTTP_CO_REQUEST_RECEIVED)
    {
        throw std::runtime_error("Attempt to send websocket message on a regular HTTP connection");
    }
    else
    {
        ws_outbox += ws_format_frame(type, msg.size());
        ws_outbox += msg;
    }
    stackout();
}

void http_co_t::reply(const std::string & reply)
{
    stackin();
    if (state != HTTP_CO_REQUEST_RECEIVED)
    {
        throw std::runtime_error("Attempt to send HTTP response in invalid connection state");
    }
    request += reply;
    submit_send();
    stackout();
}

void http_destroy(http_co_t *handler)
{
    handler->end();
}

void http_close(http_co_t *handler)
{
    handler->close_connection();
}

void http_message_t::parse_json_response(std::string & error, json11::Json & r) const
{
    if (this->error != "")
    {
        error = this->error;
        r = json11::Json();
    }
    else if (status_code != 200)
    {
        error = "HTTP "+std::to_string(status_code)+" "+status_line+" body: "+trim(body);
        r = json11::Json();
    }
    else
    {
        std::string json_err;
        json11::Json data = json11::Json::parse(body, json_err);
        if (json_err != "")
        {
            error = "Bad JSON: "+json_err+" (response: "+trim(body)+")";
            r = json11::Json();
        }
        else
        {
            error = "";
            r = data;
        }
    }
}

http_co_t::~http_co_t()
{
    close_connection();
}

void http_co_t::close_connection()
{
    if (timeout_id >= 0)
    {
        tfd->clear_timer(timeout_id);
        timeout_id = -1;
    }
    if (peer_fd >= 0)
    {
        tfd->set_fd_handler(peer_fd, false, NULL);
        close(peer_fd);
        peer_fd = -1;
    }
#ifdef WITH_OPENSSL
    if (ssl_cli)
    {
        // Frees client and bios at once
        SSL_free(ssl_cli);
        ssl_cli = NULL;
    }
    ssl_bio = NULL;
#endif
    state = HTTP_CO_CLOSED;
    connected_host = "";
    response = "";
    epoll_events = 0;
}

void http_co_t::start_ws_connection()
{
    stackin();
    resolve_and_connect();
    if (request_timeout > 0)
    {
        timeout_id = tfd->set_timer(request_timeout, false, [this](int timer_id)
        {
            stackin();
            if (state != HTTP_CO_WEBSOCKET)
            {
                close_connection();
                parsed = { .error = "Websocket connection timed out", .status_code = ETIMEDOUT };
                run_cb_and_clear();
            }
            stackout();
        });
    }
    stackout();
}

void http_ares_addrinfo_cb(void *data, int status, int timeouts, struct ares_addrinfo *result)
{
    ((http_co_t*)data)->ares_addrinfo_cb(status, timeouts, result);
}

void http_co_t::ares_addrinfo_cb(int status, int timeouts, struct ares_addrinfo *result)
{
    stackin();
    if (status != ARES_SUCCESS)
    {
        close_connection();
        parsed = { .error = ares_strerror(status), .status_code = EINVAL };
        run_cb_and_clear();
        stackout();
        return;
    }
    ares_addrinfo_node *node = NULL;
    int count = 0;
    for (node = result->nodes; node; node = node->ai_next)
    {
        count++;
    }
    assert(count > 0);
    int pos = lrand48() % count;
    for (node = result->nodes; node && pos > 0; node = node->ai_next)
    {
        pos--;
    }
    assert(node);
    start_connection(node->ai_addr, node->ai_addrlen);
    stackout();
}

void http_co_t::resolve_and_connect()
{
    stackin();
    struct sockaddr_storage addr;
    if (!string_to_addr(host.c_str(), 1, ssl ? 443 : 80, &addr))
    {
        // Try to resolve host via c-ares
        state = HTTP_CO_RESOLVING;
        ares_addrinfo_hints hints = { .ai_flags = ARES_AI_NOSORT|ARES_AI_NUMERICSERV };
        if (host_port.empty())
        {
            auto pos = host.rfind(':');
            if (pos != std::string::npos)
            {
                host_port = host.substr(pos+1);
                host = host.substr(0, pos);
            }
            else
                host_port = ssl ? "443" : "80";
        }
        ares_getaddrinfo(ctx->ares, host.c_str(), host_port.c_str(), &hints, http_ares_addrinfo_cb, this);
    }
    else
    {
        start_connection((sockaddr*)&addr, sizeof(addr));
    }
    stackout();
}

void http_co_t::start_connection(sockaddr *addr, size_t addr_size)
{
    stackin();
    peer_fd = socket(addr->sa_family, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        close_connection();
        parsed = { .error = std::string("socket: ")+strerror(errno), .status_code = errno };
        run_cb_and_clear();
        stackout();
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    epoll_events = 0;
#ifdef WITH_OPENSSL
    // https://wiki.openssl.org/index.php/Hostname_validation
    if (ssl)
    {
        if (!ctx)
            goto init_err;
        ssl_bio = BIO_new(BIO_s_socket());
        if (!ssl_bio)
            goto init_err;
        if (!BIO_set_fd(ssl_bio, peer_fd, BIO_NOCLOSE))
            goto init_err;
        ssl_cli = SSL_new(ctx->ssl_ctx);
        if (!ssl_cli)
            goto init_err;
        SSL_set_bio(ssl_cli, ssl_bio, ssl_bio);
        if (!SSL_set_tlsext_host_name(ssl_cli, host.c_str()))
        {
init_err:
            if (ssl_cli)
            {
                SSL_free(ssl_cli);
                ssl_cli = NULL;
            }
            else if (ssl_bio)
            {
                BIO_free(ssl_bio);
                ssl_bio = NULL;
            }
            parsed = { .error = std::string("openssl initialization failed: ")+ERR_error_string(ERR_get_error(), NULL) };
            response_callback(&parsed);
            response_callback = NULL;
            stackout();
            return;
        }
        SSL_set_connect_state(ssl_cli);
    }
#endif
    // Finally call connect
    int r = ::connect(peer_fd, addr, addr_size);
    if (r < 0 && errno != EINPROGRESS)
    {
        close_connection();
        parsed = { .error = std::string("connect: ")+strerror(errno), .status_code = errno };
        run_cb_and_clear();
        stackout();
        return;
    }
    tfd->set_fd_handler(peer_fd, true, [this](int peer_fd, int epoll_events)
    {
        this->epoll_events |= epoll_events;
        if (this->epoll_events & EPOLLOUT)
        {
            // Kernel sometimes reports EPOLLRDHUP before connect() actually succeeds
            // and it means absolutely nothing, it's just an erroneous event
            // EPOLLOUT, on the other hand, ALWAYS means a completed connection attempt
            this->epoll_events &= (EPOLLOUT|EPOLLIN);
        }
        handle_events();
    });
    connected_host = host;
    state = HTTP_CO_CONNECTING;
    stackout();
}

void http_co_t::handle_events()
{
    stackin();
    while (epoll_events)
    {
        if (state == HTTP_CO_CONNECTING)
        {
            if (epoll_events & (EPOLLOUT|EPOLLERR))
            {
                handle_connect_result();
            }
            else
            {
                break;
            }
        }
        else
        {
            epoll_events &= ~EPOLLOUT;
            if (epoll_events & EPOLLIN)
            {
                submit_read(false);
            }
            else if (epoll_events & (EPOLLRDHUP|EPOLLERR))
            {
                if (state == HTTP_CO_HEADERS_RECEIVED)
                    std::swap(parsed.body, response);
                else if (state == HTTP_CO_SERVER || state == HTTP_CO_REQ_HDR_RECEIVED || state == HTTP_CO_REQUEST_RECEIVED)
                    parsed = { .error = "client has disconnected normally" };
                close_connection();
                run_cb_and_clear();
                break;
            }
        }
    }
    stackout();
}

void http_co_t::handle_connect_result()
{
    stackin();
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        close_connection();
        parsed = { .error = std::string("connect: ")+strerror(result), .status_code = result };
        run_cb_and_clear();
        stackout();
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    tfd->set_fd_handler(peer_fd, false, [this](int peer_fd, int epoll_events)
    {
        this->epoll_events |= epoll_events;
        handle_events();
    });
    state = HTTP_CO_SENDING_REQUEST;
    submit_send();
    stackout();
}

void http_co_t::submit_send()
{
    stackin();
    ssize_t res = 0;
again:
    if (sent < request.size())
    {
        send_iov = (iovec){ .iov_base = (void*)(request.data()+sent), .iov_len = request.size()-sent };
#ifdef WITH_OPENSSL
        if (!ssl)
#endif
        {
            send_msg.msg_iov = &send_iov;
            send_msg.msg_iovlen = 1;
            res = sendmsg(peer_fd, &send_msg, MSG_NOSIGNAL);
            if (res < 0)
                res = -errno;
        }
#ifdef WITH_OPENSSL
        else
        {
            if (!do_ssl_handshake(false))
                goto out;
            int ok = SSL_write_ex(ssl_cli, send_iov.iov_base, send_iov.iov_len, (size_t*)&res);
            if (!ok)
            {
                res = SSL_get_error(ssl_cli, ok);
                if (res == SSL_ERROR_WANT_WRITE || res == 0)
                    res = 0;
                else if (res == SSL_ERROR_WANT_READ)
                    goto out;
                else if (res == SSL_ERROR_SYSCALL)
                    res = -errno;
                else
                {
                    on_ssl_error(res);
                    goto out;
                }
            }
        }
#endif
        if (res == -EAGAIN || res == -EINTR)
        {
            res = 0;
        }
        else if (res < 0)
        {
            close_connection();
            parsed = { .error = std::string("sendmsg: ")+strerror(errno), .status_code = errno };
            run_cb_and_clear();
            stackout();
            return;
        }
        sent += res;
        if (state == HTTP_CO_REQUEST_RECEIVED)
        {
            if (sent >= request.size())
            {
                if (!keepalive)
                {
                    close_connection();
                    parsed = { .error = "connection is not keep-alive" };
                    run_cb_and_clear();
                    stackout();
                    return;
                }
                state = HTTP_CO_SERVER;
                request = "";
                sent = 0;
            }
            else
                goto again;
            handle_read();
        }
        else if (state == HTTP_CO_SENDING_REQUEST)
        {
            if (sent >= request.size())
                state = HTTP_CO_REQUEST_SENT;
            else
                goto again;
        }
        else if (state == HTTP_CO_WEBSOCKET)
        {
            request = request.substr(sent);
            sent = 0;
            goto again;
        }
    }
out:
    stackout();
}

void http_co_t::submit_read(bool check_timeout)
{
    stackin();
    ssize_t res = 0;
again:
    if (rbuf.size() != READ_BUFFER_SIZE)
    {
        rbuf.resize(READ_BUFFER_SIZE);
    }
    read_iov = { .iov_base = rbuf.data(), .iov_len = READ_BUFFER_SIZE };
#ifdef WITH_OPENSSL
    if (!ssl)
#endif
    {
        read_msg.msg_iov = &read_iov;
        read_msg.msg_iovlen = 1;
        res = recvmsg(peer_fd, &read_msg, 0);
        if (res < 0)
            res = -errno;
    }
#ifdef WITH_OPENSSL
    else
    {
        if (!do_ssl_handshake(true))
            goto out;
        int ok = SSL_read_ex(ssl_cli, read_iov.iov_base, read_iov.iov_len, (size_t*)&res);
        if (!ok)
        {
            res = SSL_get_error(ssl_cli, ok);
            if (res == SSL_ERROR_WANT_READ)
                res = -EAGAIN;
            else if (res == SSL_ERROR_SYSCALL)
                res = -errno;
            else if (res == SSL_ERROR_ZERO_RETURN)
                res = 0;
            else
            {
                on_ssl_error(res);
                goto out;
            }
        }
    }
#endif
    if (res == -EAGAIN || res == -EINTR)
    {
        if (check_timeout)
        {
            if (res == -EINTR)
                goto again;
            else
            {
                // Timeout happened and there is no data to read
                close_connection();
                parsed = { .error = "HTTP request timed out", .status_code = ETIMEDOUT };
                run_cb_and_clear();
            }
        }
        else
        {
            epoll_events = epoll_events & ~EPOLLIN;
        }
    }
    else if (res <= 0)
    {
        // < 0 means error, 0 means EOF
        epoll_events = epoll_events & ~EPOLLIN;
        if (state == HTTP_CO_HEADERS_RECEIVED)
            std::swap(parsed.body, response);
        if (res < 0)
            parsed = { .error = std::string("recvmsg: ")+strerror(-res), .status_code = (int)-res };
        else if (state == HTTP_CO_SERVER || state == HTTP_CO_REQ_HDR_RECEIVED || state == HTTP_CO_REQUEST_RECEIVED)
            parsed = { .error = "client has disconnected normally" };
        close_connection();
        run_cb_and_clear();
    }
    else
    {
        response += std::string((char*)rbuf.data(), res);
        handle_read();
    }
out:
    stackout();
}

#ifdef WITH_OPENSSL
void http_co_t::on_ssl_error(int res)
{
    close_connection();
    if (res == SSL_ERROR_ZERO_RETURN)
    {
        // Client closed the connection
        parsed = { .error = "peer closed the SSL connection" };
    }
    else
        parsed = { .error = std::string("SSL error: ")+ERR_error_string(ERR_get_error(), NULL), .status_code = EIO };
    run_cb_and_clear();
}

bool http_co_t::do_ssl_handshake(bool init_send)
{
    if (SSL_is_init_finished(ssl_cli))
        return true;
    int r;
    while (1)
    {
        r = SSL_do_handshake(ssl_cli);
        if (r > 0)
        {
            // OK
            if (init_send)
                submit_send();
            return true;
        }
        r = SSL_get_error(ssl_cli, r);
        if (r == SSL_ERROR_WANT_READ)
        {
            break;
        }
        else
        {
            int errcode = ERR_get_error();
            parsed = { .error = ERR_error_string(errcode, NULL), .status_code = EIO };
            close_connection();
            run_cb_and_clear();
            return false;
        }
    }
    return false;
}
#endif

bool http_co_t::handle_read()
{
    stackin();
    if (state == HTTP_CO_REQUEST_RECEIVED)
    {
    }
    else if (state == HTTP_CO_REQUEST_SENT)
    {
        int pos = response.find("\r\n\r\n");
        if (pos >= 0)
        {
            if (timeout_id >= 0)
            {
                // Timeout is cleared when headers are received
                tfd->clear_timer(timeout_id);
                timeout_id = -1;
            }
            state = HTTP_CO_HEADERS_RECEIVED;
            parse_http_headers(response, &parsed, false);
            if (parsed.status_code == 101 &&
                parsed.headers.find("sec-websocket-accept") != parsed.headers.end() &&
                parsed.headers["upgrade"] == "websocket" &&
                parsed.headers["connection"] == "upgrade")
            {
                // Don't care about validating the key
                state = HTTP_CO_WEBSOCKET;
                request = ws_outbox;
                ws_outbox = "";
                sent = 0;
                submit_send();
            }
            else if (parsed.headers["transfer-encoding"] == "chunked")
            {
                state = HTTP_CO_CHUNKED;
            }
            else if (parsed.headers["connection"] != "close")
            {
                target_response_size = stoull_full(parsed.headers["content-length"]);
                if (!target_response_size)
                {
                    // Sorry, unsupported response
                    close_connection();
                    parsed = { .error = "Response has neither Connection: close, nor Transfer-Encoding: chunked nor Content-Length headers", .status_code = EINVAL };
                    run_cb_and_clear();
                    stackout();
                    return false;
                }
            }
            else
            {
                keepalive = false;
            }
        }
    }
    else if (state == HTTP_CO_SERVER)
    {
        int pos = response.find("\r\n\r\n");
        if (pos >= 0)
        {
            if (timeout_id >= 0)
            {
                // Timeout is cleared when headers are received
                tfd->clear_timer(timeout_id);
                timeout_id = -1;
            }
            state = HTTP_CO_REQ_HDR_RECEIVED;
            parse_http_headers(response, &parsed, true);
            if (ssl)
            {
                auto x509 = SSL_get0_peer_certificate(ssl_cli);
                parsed.tls_cn = openssl_get_cn(x509);
                if (ctx->ssl_ca_certs.size() > 1)
                {
                    int ca_idx = 0;
                    auto hash = X509_issuer_name_hash(x509);
                    for (X509 *ca: ctx->ssl_ca_certs)
                    {
                        if (hash == X509_subject_name_hash(ca) &&
                            X509_verify(x509, X509_get0_pubkey(ca)) > 0)
                        {
                            break;
                        }
                        ca_idx++;
                    }
                    parsed.tls_ca_idx = ca_idx;
                }
            }
            auto conn_it = parsed.headers.find("connection");
            keepalive = (conn_it != parsed.headers.end() && conn_it->second == "keep-alive");
            auto enc_it = parsed.headers.find("transfer-encoding");
            if (enc_it != parsed.headers.end())
            {
                // Sorry, unsupported request
                close_connection();
                parsed = { .error = "Chunked requests are not supported", .status_code = EINVAL };
                run_cb_and_clear();
                stackout();
                return false;
            }
            auto len_it = parsed.headers.find("content-length");
            target_response_size = stoull_full(len_it != parsed.headers.end() ? len_it->second : "");
            if (!target_response_size)
            {
                state = HTTP_CO_REQUEST_RECEIVED;
                response_callback(&parsed);
            }
        }
    }
    if ((state == HTTP_CO_HEADERS_RECEIVED || state == HTTP_CO_REQ_HDR_RECEIVED) &&
        target_response_size > 0 && response.size() >= target_response_size)
    {
        std::swap(parsed.body, response);
        if (state == HTTP_CO_REQ_HDR_RECEIVED)
        {
            state = HTTP_CO_REQUEST_RECEIVED;
            response_callback(&parsed);
        }
        else
        {
            if (!keepalive)
                close_connection();
            else
                state = HTTP_CO_KEEPALIVE;
            run_cb_and_clear();
        }
    }
    else if (state == HTTP_CO_CHUNKED && response.size() > 0)
    {
        int prev = 0, pos = 0;
        while ((pos = response.find("\r\n", prev)) >= prev)
        {
            uint64_t len = strtoull(response.c_str()+prev, NULL, 16);
            if (!len)
            {
                // Zero length chunk indicates EOF
                parsed.eof = true;
                break;
            }
            if (response.size() < pos+2+len+2)
            {
                break;
            }
            parsed.body += response.substr(pos+2, len);
            prev = pos+2+len+2;
        }
        if (prev > 0)
        {
            response = response.substr(prev);
        }
        if (want_streaming)
        {
            // Streaming response
            response_callback(&parsed);
            parsed.body = "";
        }
        else if (parsed.eof)
        {
            // Normal response
            if (!keepalive)
                close_connection();
            else
                state = HTTP_CO_KEEPALIVE;
            run_cb_and_clear();
        }
    }
    else if (state == HTTP_CO_WEBSOCKET && response.size() > 0)
    {
        while (ws_parse_frame(response, parsed.ws_msg_type, parsed.body))
        {
            if (parsed.ws_msg_type == WS_PING)
            {
                // Reply with WS_PONG
                post_message(WS_PONG, "");
            }
            else
                response_callback(&parsed);
            parsed.body = "";
        }
    }
    stackout();
    return true;
}

void http_co_t::next_request()
{
    if (keepalive_queue.size() > 0)
    {
        auto next = std::move(keepalive_queue[0]);
        keepalive_queue.erase(keepalive_queue.begin());
        send_request(next.host, next.request, next.options, next.cb);
    }
}

static void parse_http_headers(std::string & res, http_message_t *parsed, bool is_request)
{
    int pos = res.find("\r\n");
    pos = pos < 0 ? res.length() : pos+2;
    std::string status_line = res.substr(0, pos);
    int http_version;
    char *status_text = NULL;
    if (!is_request)
    {
        sscanf(status_line.c_str(), "HTTP/1.%d %d %ms", &http_version, &parsed->status_code, &status_text);
        if (status_text)
        {
            parsed->status_line = status_text;
            // %ms = allocate a buffer
            free(status_text);
            status_text = NULL;
        }
    }
    else
    {
        // Should be GET/POST / HTTP/1.1
        parsed->status_line = status_line;
    }
    int prev = pos;
    while ((pos = res.find("\r\n", prev)) >= prev)
    {
        if (pos == prev)
        {
            res = res.substr(pos+2);
            break;
        }
        std::string header = res.substr(prev, pos-prev);
        int p2 = header.find(":");
        if (p2 >= 0)
        {
            std::string key = strtolower(header.substr(0, p2));
            int p3 = p2+1;
            while (p3 < header.length() && isblank(header[p3]))
                p3++;
            parsed->headers[key] = key == "connection" || key == "upgrade" || key == "transfer-encoding"
                ? strtolower(header.substr(p3)) : header.substr(p3);
        }
        prev = pos+2;
    }
}

static std::string ws_format_frame(int type, uint64_t size)
{
    // Always zero mask
    std::string res;
    int p = 0;
    res.resize(2 + (size >= 126 ? 2 : 0) + (size >= 65536 ? 6 : 0) + /*mask*/4);
    res[p++] = 0x80 | type;
    if (size < 126)
        res[p++] = size | /*mask*/0x80;
    else if (size < 65536)
    {
        res[p++] = (char)(126 | /*mask*/0x80);
        res[p++] = (size >> 8) & 0xFF;
        res[p++] = (size >> 0) & 0xFF;
    }
    else
    {
        res[p++] = (char)(127 | /*mask*/0x80);
        res[p++] = (size >> 56) & 0xFF;
        res[p++] = (size >> 48) & 0xFF;
        res[p++] = (size >> 40) & 0xFF;
        res[p++] = (size >> 32) & 0xFF;
        res[p++] = (size >> 24) & 0xFF;
        res[p++] = (size >> 16) & 0xFF;
        res[p++] = (size >>  8) & 0xFF;
        res[p++] = (size >>  0) & 0xFF;
    }
    res[p++] = 0;
    res[p++] = 0;
    res[p++] = 0;
    res[p++] = 0;
    return res;
}

static bool ws_parse_frame(std::string & buf, uint8_t & type, std::string & res)
{
    uint64_t hdr = 2;
    if (buf.size() < hdr)
    {
        return false;
    }
    type = buf[0] & ~0x80;
    bool mask = !!(buf[1] & 0x80);
    hdr += mask ? 4 : 0;
    uint64_t len = ((uint8_t)buf[1] & ~0x80);
    if (len == 126)
    {
        hdr += 2;
        if (buf.size() < hdr)
        {
            return false;
        }
        len = ((uint64_t)(uint8_t)buf[2] << 8) | ((uint64_t)(uint8_t)buf[3] << 0);
    }
    else if (len == 127)
    {
        hdr += 8;
        if (buf.size() < hdr)
        {
            return false;
        }
        len = ((uint64_t)(uint8_t)buf[2] << 56) |
            ((uint64_t)(uint8_t)buf[3] << 48) |
            ((uint64_t)(uint8_t)buf[4] << 40) |
            ((uint64_t)(uint8_t)buf[5] << 32) |
            ((uint64_t)(uint8_t)buf[6] << 24) |
            ((uint64_t)(uint8_t)buf[7] << 16) |
            ((uint64_t)(uint8_t)buf[8] << 8) |
            ((uint64_t)(uint8_t)buf[9] << 0);
    }
    if (buf.size() < hdr+len)
    {
        return false;
    }
    if (mask)
    {
        for (int i = 0; i < len; i++)
            buf[hdr+i] ^= buf[hdr-4+(i & 3)];
    }
    res += buf.substr(hdr, len);
    buf = buf.substr(hdr+len);
    return true;
}
