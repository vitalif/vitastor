// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once
#include <string>
#include <vector>
#include <map>
#include <functional>
#include "json11/json11.hpp"

#define WS_CONTINUATION 0
#define WS_TEXT 1
#define WS_BINARY 2
#define WS_CLOSE 8
#define WS_PING 9
#define WS_PONG 10

class timerfd_manager_t;

struct http_options_t
{
    int timeout;
    bool want_streaming;
    bool keepalive;
    bool ssl;
};

struct http_context_t;

struct http_message_t
{
    std::string error;

    bool eof = false;
    int status_code = 0;
    std::string status_line;
    std::map<std::string, std::string> headers;
    uint8_t ws_msg_type = -1;
    std::string body;

    void parse_json_response(std::string & error, json11::Json & r) const;
};

// Opened websocket or keepalive HTTP connection
struct http_co_t;

http_context_t* http_context_init(const std::string & ssl_cert, const std::string & ssl_key, const std::string & ssl_ca, std::string & error);
void http_context_destroy(http_context_t *ctx);
http_co_t* http_init(timerfd_manager_t *tfd, http_context_t *ctx = NULL);
void open_websocket(http_co_t *handler, const std::string & host, const std::string & path,
    const http_options_t & options, std::function<void(const http_message_t *msg)> on_message);
void http_request(http_co_t *handler, const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(const http_message_t *response)> response_callback);
void http_post_message(http_co_t *handler, uint8_t type, const std::string & msg);
void http_close(http_co_t *co);
void http_destroy(http_co_t *co);
