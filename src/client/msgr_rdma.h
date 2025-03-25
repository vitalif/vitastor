// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once
#include <infiniband/verbs.h>
#include <string>
#include <vector>
#include "addr_util.h"

struct msgr_rdma_address_t
{
    ibv_gid gid;
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;

    std::string to_string();
    static bool from_string(const char *str, msgr_rdma_address_t *dest);
};

struct msgr_rdma_context_t
{
    ibv_context *context = NULL;
    ibv_device *dev = NULL;
    ibv_device_attr_ex attrx;
    ibv_pd *pd = NULL;
    bool odp = false;
    ibv_mr *mr = NULL;
    ibv_comp_channel *channel = NULL;
    ibv_cq *cq = NULL;
    ibv_port_attr portinfo;
    uint8_t ib_port;
    uint8_t gid_index;
    uint16_t my_lid;
    ibv_gid my_gid;
    uint32_t mtu;
    int max_cqe = 0;
    int used_max_cqe = 0;

    static msgr_rdma_context_t *create(const std::vector<addr_mask_t> & osd_network_masks, const char *ib_devname, uint8_t ib_port, int gid_index, uint32_t mtu, bool odp, int log_level);
    ~msgr_rdma_context_t();
};

struct msgr_rdma_buf_t
{
    void *buf = NULL;
    ibv_mr *mr = NULL;
};

struct msgr_rdma_connection_t
{
    msgr_rdma_context_t *ctx = NULL;
    ibv_qp *qp = NULL;
    msgr_rdma_address_t addr;
    int max_send = 0, max_recv = 0, max_sge = 0;
    int cur_send = 0, cur_recv = 0;
    uint64_t max_msg = 0;

    int send_pos = 0, send_buf_pos = 0;
    int next_recv_buf = 0;
    std::vector<msgr_rdma_buf_t> recv_buffers;
    std::vector<uint64_t> send_sizes;
    msgr_rdma_buf_t send_out;
    int send_out_pos = 0, send_done_pos = 0, send_out_size = 0;
    bool send_out_full = false;

    ~msgr_rdma_connection_t();
    static msgr_rdma_connection_t *create(msgr_rdma_context_t *ctx, uint32_t max_send, uint32_t max_recv, uint32_t max_sge, uint32_t max_msg);
    int connect(msgr_rdma_address_t *dest);
};
