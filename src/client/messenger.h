// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <sys/types.h>
#include <stdint.h>
#include <arpa/inet.h>

#include <set>
#include <map>
#include <deque>
#include <vector>

#ifdef WITH_OPENSSL
#include <openssl/types.h>
#endif

#include "../util/xxh_x86dispatch.h"
#include "../util/robin_hood.h"
#include "malloc_or_die.h"
#include "json11/json11.hpp"
#include "msgr_op.h"
#include "timerfd_manager.h"
#include "addr_util.h"
#include <ringloop.h>

#define CL_READ_HDR 1
#define CL_READ_DATA 2
#define CL_READ_REPLY_DATA 3
#define CL_WRITE_READY 1

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2
#define PEER_RDMA_CONNECTING 3
#define PEER_RDMA 4
#define PEER_STOPPED 5

#define MSGR_CSUM_PAYLOAD 1
#define MSGR_CSUM_FULL 2
#define MSGR_CSUM_NEG 4

#define VITASTOR_CONFIG_PATH "/etc/vitastor/vitastor.conf"

#define DEFAULT_MIN_ZEROCOPY_SEND_SIZE 32*1024

#define MAX_SIMPLE_PAYLOAD_SIZE 1048576

struct msgr_sendp_t
{
    osd_op_t *op;
    int flags;
};

#ifdef WITH_RDMA
struct msgr_rdma_connection_t;
struct msgr_rdma_context_t;
#endif

struct op_aes_xts_encrypt_t;
struct op_aes_xts_decrypt_t;
void destroy_aes_xts_encrypt(op_aes_xts_encrypt_t *encrypt_ctx);
void destroy_aes_xts_decrypt(op_aes_xts_decrypt_t *decrypt_ctx);

// Standard TLS record header. We are only interested in the record size
struct __attribute__((__packed__)) msgr_tls_record_hdr_t
{
    uint8_t content_type;
    uint16_t version;
    uint16_t size;
};

struct osd_client_t
{
    uint64_t client_id = 0;
    int refs = 0;

    sockaddr_storage peer_addr = {};
    int peer_port = 0;
    int peer_fd = -1;
    int peer_state = 0;
    int connect_timeout_id = -1;
    int ping_time_remaining = 0;
    int idle_time_remaining = 0;
    osd_num_t osd_num = 0;
    osd_num_t in_osd_num = 0;
    bool is_incoming = false;

    uint8_t *in_buf = NULL;

#ifdef WITH_RDMA
    msgr_rdma_connection_t *rdma_conn = NULL;
#endif

#ifdef WITH_OPENSSL
    SSL *ssl_cli = NULL;
    BIO *write_to_ssl = NULL;
    // FIXME: use custom bio to avoid 1 more memory copy?
    BIO *read_from_ssl = NULL;
    uint8_t *ssl_out_buf = NULL;
    size_t ssl_out_buf_size = 0, ssl_out_buf_cap = 0;
    bool ssl_handshake_done = false;
    msgr_tls_record_hdr_t ssl_read_record;
    size_t ssl_read_header_size = 0;
    bool ssl_more_to_buffer = false;

    EVP_CIPHER_CTX *enc_ctx = NULL;
    uint8_t enc_tag[16];
    size_t enc_tag_size = 0;
    EVP_CIPHER_CTX *dec_ctx = NULL;
    uint8_t dec_tag[16];
    size_t dec_tag_size = 0;
#endif

    // Read state
    bool io_error = false;
    int read_ready = 0;
    osd_op_t *read_op = NULL;
    size_t read_op_size = 0;
    size_t read_op_pos = 0;
    iovec read_iov = { 0 };
    msghdr read_msg = { 0 };
    std::vector<iovec> recv_list;
    std::vector<int> recv_flags;
    uint64_t read_op_id = 1;
    bool check_sequencing = false;
    bool enable_pg_locks = false;
    op_aes_xts_decrypt_t *decrypt_ctx = NULL;
    size_t read_op_inline_decrypt_pos = 0;
    size_t read_op_inline_decrypt_in = 0;
    int proto_csum_status = 0;
    XXH3_state_t* read_csum_state = NULL;

    // Incoming operations
    std::vector<osd_op_t*> received_ops;

    // Outbound operations
    robin_hood::unordered_flat_map<uint64_t, osd_op_t*> sent_ops;
    uint64_t send_op_id = 0;

    // PGs dirtied by this client's primary-writes
    std::set<pool_pg_num_t> dirty_pgs;

    // Write state
    std::deque<osd_op_t *> write_ops;
    osd_op_t *write_op = NULL;
    size_t write_op_pos = 0;
    msghdr write_msg = { 0 };
    int write_state = 0;
    std::vector<iovec> send_list;
    size_t send_list_size = 0;
    std::deque<osd_op_t*> send_free_ops;
    std::vector<osd_op_t*> zc_free_list;
    op_aes_xts_encrypt_t *encrypt_ctx = NULL;
    XXH3_state_t* write_csum_state = NULL;

    ~osd_client_t();
    void cancel_ops();
};

struct osd_wanted_peer_t
{
    json11::Json raw_address_list;
    json11::Json address_list;
    int port = 0;
    // FIXME: Remove separate WITH_RDMACM?
#ifdef WITH_RDMACM
    int rdmacm_port = 0;
#endif
    time_t last_connect_attempt = 0;
    bool connecting = false, address_changed = false;
    int address_index = 0;
    std::string cur_addr;
    int cur_port = 0;
};

struct osd_op_stats_t
{
    uint64_t op_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_count[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_bytes[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_count[OSD_OP_MAX+1] = { 0 };
};

class msgr_iothread_t;

#ifdef WITH_RDMA
struct rdma_event_channel;
struct rdma_cm_id;
struct rdma_cm_event;
struct ibv_context;
struct osd_messenger_t;
struct rdmacm_connecting_t;
#endif

class msgr_op_reader_t;
class msgr_op_writer_t;

struct __attribute__((visibility("default"))) osd_messenger_t
{
protected:
    friend class copy_op_reader_t;
    friend class ssl_op_reader_t;
    friend class gcm_op_reader_t;
    friend class get_op_reader_t;
    friend class copy_op_writer_t;
    friend class ssl_op_writer_t;
    friend class gcm_op_writer_t;
    friend class get_op_writer_t;

    int keepalive_timer_id = -1;

    uint32_t receive_buffer_size = 0;
    int peer_connect_interval = 0;
    int peer_connect_timeout = 0;
    int osd_idle_timeout = 0;
    int osd_ping_timeout = 0;
    int log_level = 0;
    bool use_sync_send_recv = false;
    int min_zerocopy_send_size = DEFAULT_MIN_ZEROCOPY_SEND_SIZE;
    int iothread_count = 0;
    int max_aes_xts_pool_size = 256;

    std::string tls_cert;
    std::string tls_key;
    std::string osd_tls_ca;
    std::string client_tls_ca;
    std::string test_osd_aes_key; // FIXME Insecure, only for PoC tests

#ifdef WITH_RDMA
    bool use_rdma = true;
    bool use_rdmacm = false;
    bool disable_tcp = false;
    std::string rdma_device;
    uint64_t rdma_port_num = 1;
    int rdma_mtu = 0;
    int rdma_gid_index = -1;
    std::vector<msgr_rdma_context_t *> rdma_contexts;
    uint64_t rdma_max_sge = 0, rdma_max_send = 0, rdma_max_recv = 0;
    uint64_t rdma_max_msg = 0;
    rdma_event_channel *rdmacm_evch = NULL;
    robin_hood::unordered_flat_map<rdma_cm_id*, osd_client_t*> rdmacm_connections;
    robin_hood::unordered_flat_map<rdma_cm_id*, rdmacm_connecting_t*> rdmacm_connecting;
#endif

#ifdef WITH_OPENSSL
    SSL_CTX *ssl_ctx = NULL;
    std::string tls_cn;

    void ssl_init(osd_client_t *cl, bool server_mode);
    bool ssl_do_handshake(osd_client_t *cl);
#endif

    std::vector<msgr_iothread_t*> iothreads;
    std::vector<uint64_t> read_ready_clients;
    std::vector<uint64_t> write_ready_clients;
    // We don't use ringloop->set_immediate here because we may have no ringloop in client :)
    std::deque<osd_op_t*> set_immediate_ops;

    std::vector<op_aes_xts_encrypt_t*> encrypt_ctx_pool;
    std::vector<op_aes_xts_decrypt_t*> decrypt_ctx_pool;

public:
    timerfd_manager_t *tfd = NULL;
    ring_loop_i *ringloop = NULL;
    bool has_sendmsg_zc = false;
    uint64_t next_client_id = 1;
    // osd_num = 0 for client messenger, osd_num > 0 for OSD messenger
    osd_num_t osd_num = 0;
    uint32_t clean_entry_bitmap_size = 0;
    uint32_t bs_block_size = 0;
    uint32_t max_write_request_size = 0;
    robin_hood::unordered_flat_map<uint64_t, osd_client_t*> clients;
    robin_hood::unordered_flat_map<uint64_t, osd_client_t*> osd_peers;
    robin_hood::unordered_flat_map<int, osd_client_t*> clients_by_fd;
    robin_hood::unordered_flat_map<osd_num_t, osd_wanted_peer_t> wanted_peers;
    std::vector<std::string> osd_networks;
    std::vector<addr_mask_t> osd_network_masks;
    std::vector<std::string> osd_cluster_networks;
    std::vector<addr_mask_t> osd_cluster_network_masks;
    std::vector<std::string> all_osd_networks;
    std::vector<addr_mask_t> all_osd_network_masks;
    int use_proto_checksums = 0;
    // op statistics
    osd_op_stats_t stats, recovery_stats;

    void init();
    void init_iothreads();
    void parse_config(const json11::Json & config);
    void connect_peer(uint64_t osd_num, json11::Json peer_state);
    void stop_client(uint64_t client_id, bool force_delete = false);
    void destroy_client(osd_client_t *cl);
    void outbox_push(osd_op_t *cur_op);
    std::function<void(osd_op_t*)> exec_op;
    std::function<void(osd_num_t)> repeer_pgs;
    std::function<void(osd_num_t)> break_pg_locks;
    std::function<bool(osd_client_t*, json11::Json)> check_config_hook;
    void read_requests();
    void send_replies();
    void accept_connections(int listen_fd);
    void destroy_iothreads();
    ~osd_messenger_t();

    static json11::Json::object read_config(const json11::Json & config);
    static json11::Json::object merge_configs(const json11::Json::object & cli_config,
        const json11::Json::object & file_config,
        const json11::Json::object & etcd_global_config,
        const json11::Json::object & etcd_osd_config);

#ifdef WITH_RDMA
    bool is_rdma_enabled();
    json11::Json connect_rdma(uint64_t client_id, std::string rdma_address, uint64_t client_max_msg);
#endif
#ifdef WITH_RDMACM
    bool is_use_rdmacm();
    rdma_cm_id *rdmacm_listen(const std::string & bind_address, int rdmacm_port, int *bound_port, int log_level);
    void rdmacm_destroy_listener(rdma_cm_id *listener);
#endif

    void inc_op_stats(osd_op_stats_t & stats, uint64_t opcode, timespec & tv_begin, timespec & tv_end, uint64_t len);
    void measure_exec(osd_op_t *cur_op);

protected:
    void try_connect_peer(uint64_t osd_num);
    void try_connect_peer_tcp(osd_num_t peer_osd, const char *peer_host, int peer_port);
    void handle_peer_epoll(int peer_fd, int epoll_events);
    void handle_connect_epoll(int peer_fd);
    void on_connect_peer(osd_num_t peer_osd, int errcode, uint64_t client_id);
    void check_peer_config(osd_client_t *cl);
    void cancel_osd_ops(osd_client_t *cl);
    void cancel_op(osd_op_t *op);

    bool try_send(osd_client_t *cl);
    void handle_send(int result, bool prev, bool more, osd_client_t *cl);
    bool op_write_to(osd_client_t *cl, msgr_op_writer_t & wr);
    void next_write_op(osd_client_t *cl);
    bool op_write_buf(osd_client_t *cl, uint8_t *src, size_t src_len, uint8_t *dst, size_t dst_len, bool skip_csum, size_t & from, size_t & done);
    bool op_copy_data_to(osd_client_t *cl, uint8_t *dst, size_t dst_len, size_t & from, size_t & done);
    size_t copy_ops_to(osd_client_t *cl, uint8_t *dst, size_t dst_len);
    template<typename T> size_t copy_ops_to_with(osd_client_t *cl, uint8_t *dst, size_t dst_len);

    void handle_read(int result, osd_client_t *cl);
    bool handle_read_buffer(osd_client_t *cl, uint8_t *curbuf, size_t bufsize);
    template<typename T> bool handle_buffer_with(osd_client_t *cl, uint8_t *curbuf, size_t bufsize);
    bool handle_hdr(osd_client_t *cl);
    bool allocate_op_buffers(osd_client_t *cl);
    bool allocate_reply_buffers(osd_client_t *cl, osd_op_t *op);
    bool op_read_from(osd_client_t *cl, msgr_op_reader_t & rdr);
    bool handle_finished_op(osd_client_t *cl);
    void handle_immediate_ops();

    void op_encrypted_copy_buf(osd_client_t *cl, uint8_t *enc_buf, size_t enc_len, uint8_t *plain, size_t plain_len, size_t & done_plain, size_t & done_enc);
    void op_encrypt_free(osd_client_t* cl);
    void op_decrypted_copy_buf(osd_client_t *cl, uint8_t *enc_buf, size_t enc_len, uint8_t *plain, size_t plain_len, size_t & done_plain, size_t & done_enc);
    void op_decrypt_start(osd_client_t* cl);
    void op_decrypt_inline(osd_client_t* cl);
    void op_decrypt_free(osd_client_t* cl);

#ifdef WITH_RDMA
    void try_send_rdma(osd_client_t *cl);
    bool init_recv_rdma(osd_client_t *cl);
    void handle_rdma_events(msgr_rdma_context_t *rdma_context);
    msgr_rdma_context_t* choose_rdma_context(osd_client_t *cl);
    void destroy_rdma_conn(msgr_rdma_connection_t *rdma_conn);
#endif
#ifdef WITH_RDMACM
    void handle_rdmacm_events();
    msgr_rdma_context_t* rdmacm_get_context(ibv_context *verbs);
    msgr_rdma_context_t* rdmacm_create_qp(rdma_cm_id *cmid);
    void rdmacm_accept(rdma_cm_event *ev);
    void rdmacm_try_connect_peer(uint64_t peer_osd, const std::string & addr, int rdmacm_port, int fallback_tcp_port);
    void rdmacm_set_conn_timeout(rdmacm_connecting_t *conn);
    void rdmacm_on_connect_peer_error(rdma_cm_id *cmid, int res);
    void rdmacm_address_resolved(rdma_cm_event *ev);
    void rdmacm_route_resolved(rdma_cm_event *ev);
    void rdmacm_established(rdma_cm_event *ev);
#endif
};
