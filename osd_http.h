#pragma once
#include <string>
#include <vector>
#include <map>

#define WS_CONTINUATION 0
#define WS_TEXT 1
#define WS_BINARY 2
#define WS_CLOSE 8
#define WS_PING 9
#define WS_PONG 10

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
};

void parse_http_headers(std::string & res, http_response_t *parsed);
std::vector<std::string> getifaddr_list(bool include_v6 = false);
uint64_t stoull_full(const std::string & str, int base = 10);
