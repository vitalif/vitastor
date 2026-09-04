// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "osd.h"
#include "osd_rmw.h"
#include "blockstore_mock.h"
#include "etcd_state_client_mock.h"
#include "ringloop_mock.h"
#include "str_util.h"

// blockstore_mock that captures enqueued ops so tests can drive them by hand.
class capturing_bs_t: public blockstore_mock_t
{
public:
    using blockstore_mock_t::blockstore_mock_t;
    std::vector<blockstore_op_t*> queued;

    void enqueue_op(blockstore_op_t *op) override
    {
        queued.push_back(op);
    }

    blockstore_op_t *take()
    {
        assert(!queued.empty());
        auto *op = queued.front();
        queued.erase(queued.begin());
        return op;
    }

    // Pop the first queued op with the given opcode. Aborts if none.
    blockstore_op_t *take(int opcode, bool expect_exist = true)
    {
        for (auto it = queued.begin(); it != queued.end(); ++it)
        {
            if ((*it)->opcode == opcode)
            {
                auto *op = *it;
                queued.erase(it);
                return op;
            }
        }
        if (expect_exist)
        {
            fprintf(stderr, "capturing_bs_t::take: no queued op with opcode %d\n", opcode);
            abort();
        }
        return NULL;
    }
};

// Test harness for primary OSD operations.
//
// Owns the mocks (ringloop, timerfd_manager, etcd, blockstore) and exposes
// helpers that drive the OSD through phases its production lifecycle would
// otherwise reach via async etcd/network events: configuring pools through
// the etcd mock, pretending peer OSDs (dis)connected, completing peering
// LIST subops.
//
// A friend of osd_t so it can poke at PG state and message queues; tests
// themselves should only talk to the fixture's public interface (and to
// the osd_t for things like exec_op).
struct osd_test_fixture_t
{
    timerfd_manager_t *tfd = nullptr;
    ring_loop_mock_t *ringloop = nullptr;
    etcd_state_client_mock_t *st_cli = nullptr; // owned by osd_t after start()
    osd_t *osd = nullptr;
    capturing_bs_t *bs = nullptr;

    osd_test_fixture_t()
    {
        tfd = new timerfd_manager_t([](int, bool, std::function<void(int, int)>) {});
        ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [](io_uring_sqe *) {});
        st_cli = new etcd_state_client_mock_t();
    }

    ~osd_test_fixture_t()
    {
        if (osd)
            delete osd;
        delete ringloop;
        delete tfd;
    }

    // Populate the etcd mock with a single replicated pool's pool/PG config
    // BEFORE start(). osd_t's constructor-time config load will then pick it
    // up synchronously and run apply_pg_config() on construction.
    //
    // pgs[i] is the osd_set for PG i+1; primary = pgs[i][0].
    void configure_replicated_pool(pool_id_t pool_id, int pg_size, int pg_minsize,
        int pg_count, const std::vector<std::vector<osd_num_t>> & pgs)
    {
        assert((int)pgs.size() == pg_count);
        auto pool_id_s = std::to_string(pool_id);
        st_cli->set("/vitastor/config/pools", json11::Json::object {
            { pool_id_s, json11::Json::object {
                { "name", "pool_"+pool_id_s },
                { "scheme", "replicated" },
                { "pg_size", pg_size },
                { "pg_minsize", pg_minsize },
                { "pg_count", pg_count },
                { "failure_domain", "osd" },
                { "immediate_commit", "none" },
            } },
        });
        json11::Json::object items_pool;
        for (int i = 0; i < pg_count; i++)
        {
            json11::Json::array osd_set;
            for (auto n: pgs[i])
                osd_set.push_back((double)n);
            items_pool[std::to_string(i+1)] = json11::Json::object {
                { "osd_set", osd_set },
                { "primary", (double)pgs[i][0] },
            };
        }
        st_cli->set("/vitastor/pg/config", json11::Json::object {
            { "items", json11::Json::object{ { pool_id_s, items_pool } } },
        });
    }

    void configure_ec_pool(int data_chunks, int parity_chunks, uint64_t primary_osd = 2)
    {
        auto pool_id_s = std::to_string(1);
        st_cli->set("/vitastor/config/pools", json11::Json::object {
            { pool_id_s, json11::Json::object {
                { "name", "pool_1" },
                { "scheme", "ec" },
                { "pg_size", data_chunks+parity_chunks },
                { "pg_minsize", data_chunks },
                { "parity_chunks", parity_chunks },
                { "pg_count", 1 },
                { "failure_domain", "osd" },
                { "immediate_commit", "none" },
            } },
        });
        json11::Json::array osd_set;
        for (uint64_t i = 1; i <= data_chunks+parity_chunks; i++)
            osd_set.push_back(i);
        st_cli->set("/vitastor/pg/config", json11::Json::object {
            { "items", json11::Json::object{
                { pool_id_s, json11::Json::object {
                    { std::to_string(1), json11::Json::object {
                        { "osd_set", osd_set },
                        { "primary", primary_osd },
                    } }
                } }
            } },
        });
    }

    // Construct the osd_t AND call osd->start(). With the etcd mock unpaused,
    // the whole config-load chain (config/global, config/pools, lease,
    // /osd/state, pg/config, apply_pg_config, start_pg_peering) runs
    // synchronously inside start(). Peers that aren't yet connect_peer()'d
    // will leave their PGs in PG_INCOMPLETE; call connect_peer() +
    // complete_peering_empty() afterwards to drive them to PG_ACTIVE.
    //
    // If a test needs a fully inert osd_t (no timers, no etcd traffic) — for
    // instance to verify constructor-time behavior — use construct() instead
    // and call osd->start() yourself.
    void construct(json11::Json::object osd_config)
    {
        assert(!osd);
        osd = new osd_t(osd_config, ringloop, tfd,
            std::unique_ptr<etcd_state_client_t>(st_cli),
            [this](blockstore_config_t & cfg) -> blockstore_i* {
                return (bs = new capturing_bs_t(cfg));
            });
    }

    void start(json11::Json::object osd_config)
    {
        construct(osd_config);
        osd->start();
    }

    // Pretend a peer OSD has connected (no real TCP). Registers it in the
    // messenger and triggers re-peering of PGs that include it.
    void connect_peer(osd_num_t osd_num)
    {
        auto *msgr = &osd->msgr;
        auto *cl = new osd_client_t();
        cl->client_id = msgr->next_client_id++;
        cl->osd_num = osd_num;
        cl->peer_fd = -1;
        cl->peer_state = PEER_CONNECTED;
        msgr->osd_peers[osd_num] = cl;
        msgr->clients[cl->client_id] = cl;
        msgr->wanted_peers.erase(osd_num);
        msgr->repeer_pgs(osd_num);
    }

    void disconnect_peer(osd_num_t osd_num)
    {
        osd->msgr.stop_client(osd->msgr.osd_peers.at(osd_num)->client_id);
    }

    // Complete every outstanding peering LIST op (local BS_OP_LIST + peer
    // OSD_OP_SEC_LIST) with "no objects" so PGs transition to PG_ACTIVE
    // without us having to invent object lists.
    void complete_peering_empty()
    {
        for (auto it = bs->queued.begin(); it != bs->queued.end(); )
        {
            auto *op = *it;
            if (op->opcode == BS_OP_LIST)
            {
                it = bs->queued.erase(it++);
                op->retval = 0;
                op->version = 0;
                op->buf = NULL;
                std::function<void(blockstore_op_t*)>(op->callback)(op);
            }
            else
                ++it;
        }
        for (auto & p: osd->msgr.osd_peers)
        {
            auto *cl = p.second;
            std::vector<osd_op_t*> list_ops;
            for (auto & kv: cl->sent_ops)
                if (kv.second->req.hdr.opcode == OSD_OP_SEC_LIST)
                    list_ops.push_back(kv.second);
            for (auto *op: list_ops)
            {
                cl->sent_ops.erase(op->req.hdr.id);
                op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
                op->reply.hdr.id = op->req.hdr.id;
                op->reply.hdr.opcode = op->req.hdr.opcode;
                op->reply.hdr.retval = 0;
                op->reply.sec_list.stable_count = 0;
                op->buf = NULL;
                op->callback(op);
            }
        }
        // Drive handle_peers so PGs see lists_done and finalize calc_object_states.
        ringloop->wakeup();
        ringloop->loop();
    }

    // Inline list response builder. `objects` are pairs (oid, version);
    // the first `stable_count` entries are reported as stable. The buffer
    // is malloc'd and ownership transfers to the peering machinery, which
    // frees it after calc_object_states.
    static obj_ver_id *build_list_buf(const std::vector<obj_ver_id> & objects)
    {
        if (objects.empty())
            return nullptr;
        auto *buf = (obj_ver_id*)malloc(objects.size() * sizeof(obj_ver_id));
        for (size_t i = 0; i < objects.size(); i++)
            buf[i] = objects[i];
        return buf;
    }

    // Pop the first BS_OP_LIST from bs->queued and reply with the given
    // object list. The peering callback consumes (and later frees) op->buf.
    void reply_local_list(const std::vector<obj_ver_id> & objects, uint64_t stable_count)
    {
        auto *op = bs->take(BS_OP_LIST);
        op->buf = (uint8_t*)build_list_buf(objects);
        op->retval = (int)objects.size();
        op->version = stable_count;
        std::function<void(blockstore_op_t*)>(op->callback)(op);
    }

    // Pop the OSD_OP_SEC_LIST sent to `osd_num` and reply with the given
    // object list. Same ownership rules as reply_local_list.
    void reply_peer_list(osd_num_t osd_num,
        const std::vector<obj_ver_id> & objects, uint64_t stable_count)
    {
        auto *cl = osd->msgr.osd_peers.at(osd_num);
        osd_op_t *op = nullptr;
        for (auto & kv: cl->sent_ops)
        {
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_LIST)
            {
                op = kv.second;
                break;
            }
        }
        assert(op);
        cl->sent_ops.erase(op->req.hdr.id);
        op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        op->reply.hdr.id = op->req.hdr.id;
        op->reply.hdr.opcode = op->req.hdr.opcode;
        op->reply.hdr.retval = (int64_t)objects.size();
        op->reply.sec_list.stable_count = stable_count;
        op->buf = build_list_buf(objects);
        std::function<void(osd_op_t*)>(op->callback)(op);
    }

    // Pop the first sent op of given opcode from peer's outbox. Aborts if
    // none — caller should know which subops to expect.
    osd_op_t *peer_take(osd_num_t osd_num, uint64_t opcode)
    {
        auto *cl = osd->msgr.osd_peers.at(osd_num);
        for (auto & kv: cl->sent_ops)
        {
            if (kv.second->req.hdr.opcode == opcode)
            {
                return kv.second;
            }
        }
        fprintf(stderr, "peer_take: OSD %ju has no sent op with opcode %ju\n", osd_num, opcode);
        abort();
    }

    void peer_complete(osd_op_t *op, int64_t retval)
    {
        auto *cl = osd->msgr.clients.at(op->client_id);
        cl->sent_ops.erase(op->req.hdr.id);
        op->reply.hdr.retval = retval;
        op->callback(op);
    }

    // Drive ringloop to flush handle_peers and any pending lambdas.
    void pump()
    {
        ringloop->wakeup();
        ringloop->loop();
    }

    // Submit an op to the OSD's primary state machine. Wrapper because
    // osd_t::exec_op is private — tests aren't friends, the fixture is.
    void exec(osd_op_t *op)
    {
        osd->exec_op(op);
    }

    osd_client_t *peer(osd_num_t osd_num)
    {
        return osd->msgr.osd_peers.at(osd_num);
    }

    // Create a regular (non-OSD) incoming client with no real socket. Replies
    // routed to it by finish_op -> outbox_push land in cl->sent_ops (see the
    // mock messenger). Returns the new client_id.
    uint64_t connect_client()
    {
        auto *msgr = &osd->msgr;
        auto *cl = new osd_client_t();
        cl->client_id = msgr->next_client_id++;
        cl->osd_num = 0;
        cl->peer_fd = -1;
        cl->peer_state = PEER_CONNECTED;
        cl->is_incoming = true;
        msgr->clients[cl->client_id] = cl;
        return cl->client_id;
    }

    osd_client_t *client(uint64_t client_id)
    {
        auto it = osd->msgr.clients.find(client_id);
        return it == osd->msgr.clients.end() ? nullptr : it->second;
    }

    bool has_client(uint64_t client_id)
    {
        return osd->msgr.clients.find(client_id) != osd->msgr.clients.end();
    }

    pg_t &pg(pool_id_t pool, pg_num_t num)
    {
        return osd->pgs.at({ .pool_id = pool, .pg_num = num });
    }

    void bs_zero_read_ok(uint64_t version)
    {
        auto *zr = bs->take(BS_OP_READ);
        assert(zr->len == 0);
        zr->version = version;
        zr->retval = version ? 0 : -ENOENT;
        zr->callback(zr);
    }

    void bs_write_ok(uint64_t opcode, uint64_t version)
    {
        auto *wr = bs->take(opcode);
        wr->version = version;
        wr->retval = wr->len;
        wr->callback(wr);
    }

    void peer_write_ok(osd_num_t peer, uint64_t opcode, uint64_t version)
    {
        auto *pw = peer_take(peer, opcode);
        if (opcode == OSD_OP_SEC_DELETE)
        {
            pw->reply.sec_del.version = version;
            peer_complete(pw, 0);
        }
        else
        {
            pw->reply.sec_rw.version = version;
            peer_complete(pw, pw->req.sec_rw.len);
        }
    }

    bool has_pg(pool_id_t pool, pg_num_t num)
    {
        return osd->pgs.count({ .pool_id = pool, .pg_num = num }) > 0;
    }

    bool has_dirty_pg(pool_id_t pool, pg_num_t num)
    {
        return osd->dirty_pgs.count({ .pool_id = pool, .pg_num = num }) > 0;
    }

    bool has_dirty_osd(osd_num_t num)
    {
        return osd->dirty_osds.count(num) > 0;
    }

    size_t syncs_in_progress_size()
    {
        return osd->syncs_in_progress.size();
    }
};
