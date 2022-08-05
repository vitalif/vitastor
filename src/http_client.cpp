// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <arpa/inet.h>

#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include <stdexcept>

#include "addr_util.h"
#include "str_util.h"
#include "json11/json11.hpp"
#include "http_client.h"
#include "timerfd_manager.h"

#define READ_BUFFER_SIZE 9000

static std::string ws_format_frame(int type, uint64_t size);
static bool ws_parse_frame(std::string & buf, int & type, std::string & res);
static void parse_http_headers(std::string & res, http_response_t *parsed);

struct http_co_t
{
    timerfd_manager_t *tfd;
    std::function<void(const http_response_t*)> response_callback;

    int request_timeout = 0;
    std::string host;
    std::string request;
    std::string ws_outbox;
    std::string response;
    bool want_streaming;
    bool keepalive;

    std::vector<std::function<void()>> keepalive_queue;

    int state = 0;
    std::string connected_host;
    int peer_fd = -1;
    int timeout_id = -1;
    int epoll_events = 0;
    int sent = 0;
    std::vector<char> rbuf;
    iovec read_iov, send_iov;
    msghdr read_msg = { 0 }, send_msg = { 0 };
    http_response_t parsed;
    uint64_t target_response_size = 0;

    int onstack = 0;
    bool ended = false;

    ~http_co_t();
    inline void stackin() { onstack++; }
    inline void stackout() { onstack--; if (!onstack && ended) end(); }
    inline void end() { ended = true; if (!onstack) { delete this; } }
    void run_cb_and_clear();
    void start_connection();
    void close_connection();
    void next_request();
    void handle_events();
    void handle_connect_result();
    void submit_read(bool check_timeout);
    void submit_send();
    bool handle_read();
    void post_message(int type, const std::string & msg);
    void send_request(const std::string & host, const std::string & request,
        const http_options_t & options, std::function<void(const http_response_t *response)> response_callback);
};

#define HTTP_CO_CLOSED 0
#define HTTP_CO_CONNECTING 1
#define HTTP_CO_SENDING_REQUEST 2
#define HTTP_CO_REQUEST_SENT 3
#define HTTP_CO_HEADERS_RECEIVED 4
#define HTTP_CO_WEBSOCKET 5
#define HTTP_CO_CHUNKED 6
#define HTTP_CO_KEEPALIVE 7

#define DEFAULT_TIMEOUT 5000

http_co_t *http_init(timerfd_manager_t *tfd)
{
    http_co_t *handler = new http_co_t();
    handler->tfd = tfd;
    handler->state = HTTP_CO_CLOSED;
    return handler;
}

http_co_t* open_websocket(timerfd_manager_t *tfd, const std::string & host, const std::string & path,
    int timeout, std::function<void(const http_response_t *msg)> response_callback)
{
    std::string request = "GET "+path+" HTTP/1.1\r\n"
        "Host: "+host+"\r\n"
        "Upgrade: websocket\r\n"
        "Connection: upgrade\r\n"
        "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    http_co_t *handler = new http_co_t();
    handler->tfd = tfd;
    handler->state = HTTP_CO_CLOSED;
    handler->host = host;
    handler->request_timeout = timeout < 0 ? -1 : (timeout == 0 ? DEFAULT_TIMEOUT : timeout);
    handler->want_streaming = false;
    handler->keepalive = false;
    handler->request = request;
    handler->response_callback = response_callback;
    handler->start_connection();
    return handler;
}

void http_request(http_co_t *handler, const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(const http_response_t *response)> response_callback)
{
    handler->send_request(host, request, options, response_callback);
}

void http_co_t::run_cb_and_clear()
{
    parsed.eof = true;
    std::function<void(const http_response_t*)> cb;
    cb.swap(response_callback);
    // Call callback after clearing it because otherwise we may hit reenterability problems
    if (cb != NULL)
        cb(&parsed);
    next_request();
}

void http_co_t::send_request(const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(const http_response_t *response)> response_callback)
{
    stackin();
    if (state == HTTP_CO_WEBSOCKET)
    {
        stackout();
        throw std::runtime_error("Attempt to send HTTP request into a websocket or chunked stream");
    }
    else if (state != HTTP_CO_KEEPALIVE && state != HTTP_CO_CLOSED)
    {
        keepalive_queue.push_back([this, host, request, options, response_callback]()
        {
            this->send_request(host, request, options, response_callback);
        });
        stackout();
        return;
    }
    if (state == HTTP_CO_KEEPALIVE && connected_host != host)
    {
        close_connection();
    }
    this->request_timeout = options.timeout < 0 ? 0 : (options.timeout == 0 ? DEFAULT_TIMEOUT : options.timeout);
    this->want_streaming = options.want_streaming;
    this->keepalive = options.keepalive;
    this->host = host;
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
        start_connection();
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
                parsed = { .error = "HTTP request timed out" };
                run_cb_and_clear();
            }
            stackout();
        });
    }
    stackout();
}

void http_post_message(http_co_t *handler, int type, const std::string & msg)
{
    handler->post_message(type, msg);
}

void http_co_t::post_message(int type, const std::string & msg)
{
    stackin();
    if (state == HTTP_CO_WEBSOCKET)
    {
        request += ws_format_frame(type, msg.size());
        request += msg;
        submit_send();
    }
    else if (state == HTTP_CO_KEEPALIVE || state == HTTP_CO_CHUNKED)
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

void http_close(http_co_t *handler)
{
    handler->end();
}

void http_response_t::parse_json_response(std::string & error, json11::Json & r) const
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
    state = HTTP_CO_CLOSED;
    connected_host = "";
    response = "";
    epoll_events = 0;
}

void http_co_t::start_connection()
{
    stackin();
    struct sockaddr_storage addr;
    if (!string_to_addr(host.c_str(), 1, 80, &addr))
    {
        close_connection();
        parsed = { .error = "Invalid address: "+host };
        run_cb_and_clear();
        stackout();
        return;
    }
    peer_fd = socket(addr.ss_family, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        close_connection();
        parsed = { .error = std::string("socket: ")+strerror(errno) };
        run_cb_and_clear();
        stackout();
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    epoll_events = 0;
    // Finally call connect
    int r = ::connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close_connection();
        parsed = { .error = std::string("connect: ")+strerror(errno) };
        run_cb_and_clear();
        stackout();
        return;
    }
    tfd->set_fd_handler(peer_fd, true, [this](int peer_fd, int epoll_events)
    {
        this->epoll_events |= epoll_events;
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
            handle_connect_result();
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
        parsed = { .error = std::string("connect: ")+strerror(result) };
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
    int res;
again:
    if (sent < request.size())
    {
        send_iov = (iovec){ .iov_base = (void*)(request.c_str()+sent), .iov_len = request.size()-sent };
        send_msg.msg_iov = &send_iov;
        send_msg.msg_iovlen = 1;
        res = sendmsg(peer_fd, &send_msg, MSG_NOSIGNAL);
        if (res < 0)
        {
            res = -errno;
        }
        if (res == -EAGAIN || res == -EINTR)
        {
            res = 0;
        }
        else if (res < 0)
        {
            close_connection();
            parsed = { .error = std::string("sendmsg: ")+strerror(errno) };
            run_cb_and_clear();
            stackout();
            return;
        }
        sent += res;
        if (state == HTTP_CO_SENDING_REQUEST)
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
    stackout();
}

void http_co_t::submit_read(bool check_timeout)
{
    stackin();
    int res;
again:
    if (rbuf.size() != READ_BUFFER_SIZE)
    {
        rbuf.resize(READ_BUFFER_SIZE);
    }
    read_iov = { .iov_base = rbuf.data(), .iov_len = READ_BUFFER_SIZE };
    read_msg.msg_iov = &read_iov;
    read_msg.msg_iovlen = 1;
    res = recvmsg(peer_fd, &read_msg, 0);
    if (res < 0)
    {
        res = -errno;
    }
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
                parsed = { .error = "HTTP request timed out" };
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
        close_connection();
        if (res < 0)
            parsed = { .error = std::string("recvmsg: ")+strerror(-res) };
        run_cb_and_clear();
    }
    else
    {
        response += std::string(rbuf.data(), res);
        handle_read();
    }
    stackout();
}

bool http_co_t::handle_read()
{
    stackin();
    if (state == HTTP_CO_REQUEST_SENT)
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
            parse_http_headers(response, &parsed);
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
                    parsed = { .error = "Response has neither Connection: close, nor Transfer-Encoding: chunked nor Content-Length headers" };
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
    if (state == HTTP_CO_HEADERS_RECEIVED && target_response_size > 0 && response.size() >= target_response_size)
    {
        std::swap(parsed.body, response);
        if (!keepalive)
            close_connection();
        else
            state = HTTP_CO_KEEPALIVE;
        run_cb_and_clear();
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
        auto next = keepalive_queue[0];
        keepalive_queue.erase(keepalive_queue.begin(), keepalive_queue.begin()+1);
        next();
    }
}

static void parse_http_headers(std::string & res, http_response_t *parsed)
{
    int pos = res.find("\r\n");
    pos = pos < 0 ? res.length() : pos+2;
    std::string status_line = res.substr(0, pos);
    int http_version;
    char *status_text = NULL;
    sscanf(status_line.c_str(), "HTTP/1.%d %d %ms", &http_version, &parsed->status_code, &status_text);
    if (status_text)
    {
        parsed->status_line = status_text;
        // %ms = allocate a buffer
        free(status_text);
        status_text = NULL;
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

static bool ws_parse_frame(std::string & buf, int & type, std::string & res)
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

// FIXME: move to utils
bool json_is_true(const json11::Json & val)
{
    if (val.is_string())
        return val == "true" || val == "yes" || val == "1";
    return val.bool_value();
}

bool json_is_false(const json11::Json & val)
{
    if (val.is_string())
        return val.string_value() == "false" || val.string_value() == "no" || val.string_value() == "0";
    if (val.is_number())
        return val.number_value() == 0;
    if (val.is_bool())
        return !val.bool_value();
    return false;
}
