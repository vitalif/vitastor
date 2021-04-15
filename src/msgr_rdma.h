#pragma once
#include <infiniband/verbs.h>
#include <string>
#include <vector>

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

    static msgr_rdma_context_t *create(const char *ib_devname, uint8_t ib_port, uint8_t gid_index, uint32_t mtu);
    ~msgr_rdma_context_t();
};

struct msgr_rdma_connection_t
{
    msgr_rdma_context_t *ctx = NULL;
    ibv_qp *qp = NULL;
    msgr_rdma_address_t addr;
    int max_send = 0, max_recv = 0, max_sge = 0;
    int cur_send = 0, cur_recv = 0;

    int send_pos = 0, send_buf_pos = 0;
    int recv_pos = 0, recv_buf_pos = 0;

    ~msgr_rdma_connection_t();
    static msgr_rdma_connection_t *create(msgr_rdma_context_t *ctx, uint32_t max_send, uint32_t max_recv, uint32_t max_sge);
    int connect(msgr_rdma_address_t *dest);
};
