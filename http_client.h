// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

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
};

struct http_response_t
{
    bool eof = false;
    int error_code = 0;
    int status_code = 0;
    std::string status_line;
    std::map<std::string, std::string> headers;
    int ws_msg_type = -1;
    std::string body;
};

struct http_co_t;

struct websocket_t
{
    http_co_t *co;
    void post_message(int type, const std::string & msg);
    void close();
};

void parse_http_headers(std::string & res, http_response_t *parsed);

std::vector<std::string> getifaddr_list(bool include_v6 = false);

uint64_t stoull_full(const std::string & str, int base = 10);

void http_request(timerfd_manager_t *tfd, const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(const http_response_t *response)> callback);

void http_request_json(timerfd_manager_t *tfd, const std::string & host, const std::string & request,
    int timeout, std::function<void(std::string, json11::Json r)> callback);

websocket_t* open_websocket(timerfd_manager_t *tfd, const std::string & host, const std::string & path,
    int timeout, std::function<void(const http_response_t *msg)> callback);
