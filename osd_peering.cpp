#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "osd.h"

void osd_t::init_primary()
{
    // Initial test version of clustering code requires exactly 2 peers
    if (config["peer1"] == "" || config["peer2"] == "")
        throw std::runtime_error("run_primary requires two peers");
    peers.push_back(parse_peer(config["peer1"]));
    peers.push_back(parse_peer(config["peer2"]));
    if (peers[1].osd_num == peers[0].osd_num)
        throw std::runtime_error("peer1 and peer2 osd numbers are the same");
    pgs.push_back((osd_pg_t){
        .state = PG_OFFLINE,
        .pg_num = 1,
        .target_set = { 1, 2, 3 },
        .obj_states = spp::sparse_hash_map<object_id, const osd_obj_state_t*>(),
        .ver_override = spp::sparse_hash_map<object_id, osd_ver_override_t>(),
    });
    pg_count = 1;
    peering_state = 1;
}

osd_peer_def_t osd_t::parse_peer(std::string peer)
{
    // OSD_NUM:IP:PORT
    size_t pos1 = peer.find(':');
    size_t pos2 = peer.find(':', pos1+1);
    if (pos1 < 0 || pos2 < 0)
        throw new std::runtime_error("OSD peer string must be in the form OSD_NUM:IP:PORT");
    osd_peer_def_t r;
    r.addr = peer.substr(pos1+1, pos2-pos1-1);
    std::string osd_num_str = peer.substr(0, pos1);
    std::string port_str = peer.substr(pos2+1);
    r.osd_num = strtoull(osd_num_str.c_str(), NULL, 10);
    if (!r.osd_num)
        throw new std::runtime_error("Could not parse OSD peer osd_num");
    r.port = strtoull(port_str.c_str(), NULL, 10);
    if (!r.port)
        throw new std::runtime_error("Could not parse OSD peer port");
    return r;
}

void osd_t::connect_peer(unsigned osd_num, const char *peer_host, int peer_port, std::function<void(int)> callback)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, peer_host, &addr.sin_addr)) != 1)
    {
        callback(-EINVAL);
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port ? peer_port : 11203);
    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        callback(-errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        callback(-errno);
        return;
    }
    clients[peer_fd] = (osd_client_t){
        .peer_addr = addr,
        .peer_port = peer_port,
        .peer_fd = peer_fd,
        .peer_state = PEER_CONNECTING,
        .connect_callback = callback,
        .osd_num = osd_num,
    };
    osd_peer_fds[osd_num] = peer_fd;
    // Add FD to epoll (EPOLLOUT for tracking connect() result)
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
}

void osd_t::handle_connect_result(int peer_fd)
{
    auto & cl = clients[peer_fd];
    std::function<void(int)> callback = cl.connect_callback;
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        stop_client(peer_fd);
        callback(-result);
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    // Disable EPOLLOUT on this fd
    cl.connect_callback = NULL;
    cl.peer_state = PEER_CONNECTED;
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    callback(peer_fd);
}

// Peering loop
// Ideally: Connect -> Ask & check config -> Start PG peering
void osd_t::handle_peers()
{
    if (peering_state & 1)
    {
        for (int i = 0; i < peers.size(); i++)
        {
            if (osd_peer_fds.find(peers[i].osd_num) == osd_peer_fds.end() &&
                time(NULL) - peers[i].last_connect_attempt > 5)
            {
                peers[i].last_connect_attempt = time(NULL);
                connect_peer(peers[i].osd_num, peers[i].addr.c_str(), peers[i].port, [this](int peer_fd)
                {
                    printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                    int i;
                    for (i = 0; i < peers.size(); i++)
                    {
                        auto it = osd_peer_fds.find(peers[i].osd_num);
                        if (it == osd_peer_fds.end() || clients[it->second].peer_state != PEER_CONNECTED)
                        {
                            break;
                        }
                    }
                    if (i >= peers.size())
                    {
                        // Start PG peering
                        pgs[0].state = PG_PEERING;
                        pgs[0].state_dict.clear();
                        pgs[0].obj_states.clear();
                        pgs[0].ver_override.clear();
                        if (pgs[0].peering_state)
                            delete pgs[0].peering_state;
                        peering_state = 2;
                        ringloop->wakeup();
                    }
                });
            }
        }
    }
    if (peering_state & 2)
    {
        for (int i = 0; i < pgs.size(); i++)
        {
            if (pgs[i].state == PG_PEERING)
            {
                if (!pgs[i].peering_state)
                {
                    start_pg_peering(i);
                }
                else if (pgs[i].peering_state->list_done >= 3)
                {
                    calc_object_states(pgs[i]);
                    peering_state = 0;
                }
            }
        }
    }
}

void osd_t::start_pg_peering(int pg_idx)
{
    auto & pg = pgs[pg_idx];
    auto ps = pg.peering_state = new osd_pg_peering_state_t();
    ps->self = this;
    ps->pg_num = pg_idx; // FIXME probably shouldn't be pg_idx
    {
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        op->bs_op.opcode = BS_OP_LIST;
        op->bs_op.callback = [ps, op](blockstore_op_t *bs_op)
        {
            printf(
                "Got object list from OSD %lu (local): %d objects (%lu of them stable)\n",
                ps->self->osd_num, bs_op->retval, bs_op->version
            );
            op->buf = op->bs_op.buf;
            op->reply.hdr.retval = op->bs_op.retval;
            op->reply.sec_list.stable_count = op->bs_op.version;
            ps->list_done++;
        };
        pg.peering_state->list_ops[osd_num] = op;
        bs->enqueue_op(&op->bs_op);
    }
    for (int i = 0; i < peers.size(); i++)
    {
        auto & cl = clients[osd_peer_fds[peers[i].osd_num]];
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = cl.peer_fd;
        op->op = {
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = 1,
                    .opcode = OSD_OP_SECONDARY_LIST,
                },
                .pgnum = 1,
                .pgtotal = 1,
            },
        };
        op->callback = [ps](osd_op_t *op)
        {
            printf(
                "Got object list from OSD %lu: %ld objects (%lu of them stable)\n",
                ps->self->clients[op->peer_fd].osd_num, op->reply.hdr.retval,
                op->reply.sec_list.stable_count
            );
            ps->list_done++;
        };
        pg.peering_state->list_ops[cl.osd_num] = op;
        outbox_push(cl, op);
    }
}

void osd_t::remember_object(osd_pg_t &pg, osd_obj_state_check_t &st, std::vector<obj_ver_role> &all, int end)
{
    // Remember the decision
    uint64_t state = 0;
    if (st.n_roles == pg.pg_size)
    {
        if (st.n_matched == pg.pg_size)
            state = OBJ_CLEAN;
        else
            state = OBJ_MISPLACED;
    }
    else if (st.n_roles < pg.pg_minsize)
        state = OBJ_INCOMPLETE;
    else
        state = OBJ_DEGRADED;
    if (st.n_copies > pg.pg_size)
        state |= OBJ_OVERCOPIED;
    if (st.n_stable < st.n_copies)
        state |= OBJ_NONSTABILIZED;
    if (st.target_ver < st.max_ver)
        state |= OBJ_UNDERWRITTEN;
    if (st.is_buggy)
        state |= OBJ_BUGGY;
    if (state != OBJ_CLEAN)
    {
        st.state_obj.state = state;
        st.state_obj.loc.clear();
        for (int i = st.start; i < end; i++)
        {
            st.state_obj.loc.push_back((osd_obj_loc_t){
                .role = (all[i].oid.stripe & STRIPE_MASK),
                .osd_num = all[i].osd_num,
                .stable = all[i].is_stable,
            });
        }
        std::sort(st.state_obj.loc.begin(), st.state_obj.loc.end());
        auto ins = pg.state_dict.insert(st.state_obj);
        pg.obj_states[st.oid] = &(*(ins.first));
        if (state & OBJ_UNDERWRITTEN)
        {
            pg.ver_override[st.oid] = {
                .max_ver = st.max_ver,
                .target_ver = st.target_ver,
            };
        }
    }
}

void osd_t::calc_object_states(osd_pg_t &pg)
{
    // Copy all object lists into one array
    std::vector<obj_ver_role> all;
    auto ps = pg.peering_state;
    for (auto e: ps->list_ops)
    {
        osd_op_t* op = e.second;
        auto nstab = op->reply.sec_list.stable_count;
        auto n = op->reply.hdr.retval;
        auto osd_num = clients[op->peer_fd].osd_num;
        all.resize(all.size() + n);
        obj_ver_id *ov = (obj_ver_id*)op->buf;
        for (uint64_t i = 0; i < n; i++, ov++)
        {
            all[i] = {
                .oid = ov->oid,
                .version = ov->version,
                .osd_num = osd_num,
                .is_stable = i < nstab,
            };
        }
        free(op->buf);
        op->buf = NULL;
    }
    // Sort
    std::sort(all.begin(), all.end());
    // Walk over it and check object states
    int replica = 0;
    osd_obj_state_check_t st;
    for (int i = 0; i < all.size(); i++)
    {
        if (st.oid.inode != all[i].oid.inode ||
            st.oid.stripe != (all[i].oid.stripe >> STRIPE_SHIFT))
        {
            if (st.oid.inode != 0)
            {
                // Remember object state
                remember_object(pg, st, all, i);
            }
            st.start = i;
            st.oid = { .inode = all[i].oid.inode, .stripe = all[i].oid.stripe >> STRIPE_SHIFT };
            st.max_ver = st.target_ver = all[i].version;
            st.has_roles = st.n_copies = st.n_roles = st.n_stable = st.n_matched = 0;
            st.is_buggy = false;
        }
        if (st.target_ver != all[i].version)
        {
            if (st.n_stable > 0 || st.n_roles >= pg.pg_minsize)
            {
                // Version is either recoverable or stable, choose it as target and skip previous versions
                remember_object(pg, st, all, i);
                while (i < all.size() && st.oid.inode == all[i].oid.inode &&
                    st.oid.stripe == (all[i].oid.stripe >> STRIPE_SHIFT))
                {
                    i++;
                }
                continue;
            }
            else
            {
                // Remember that there are newer unrecoverable versions
                st.target_ver = all[i].version;
                st.has_roles = st.n_copies = st.n_roles = st.n_stable = st.n_matched = 0;
            }
        }
        replica = (all[i].oid.stripe & STRIPE_MASK);
        st.n_copies++;
        if (replica >= pg.pg_size)
        {
            // FIXME In the future, check it against the PG epoch number to handle replication factor/scheme changes
            st.is_buggy = true;
        }
        else
        {
            if (all[i].is_stable)
            {
                st.n_stable++;
            }
            else if (pg.target_set[replica] == all[i].osd_num)
            {
                st.n_matched++;
            }
            if (!(st.has_roles & (1 << replica)))
            {
                st.has_roles = st.has_roles | (1 << replica);
                st.n_roles++;
            }
        }
    }
    if (st.oid.inode != 0)
    {
        // Remember object state
        remember_object(pg, st, all, all.size());
    }
}
