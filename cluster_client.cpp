#include "cluster_client.h"

cluster_client_t::cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json & config)
{
    this->ringloop = ringloop;
    this->tfd = tfd;

    msgr.tfd = tfd;
    msgr.ringloop = ringloop;
    msgr.repeer_pgs = [this](osd_num_t peer_osd)
    {
        // peer_osd just connected or dropped connection
        if (msgr.osd_peer_fds.find(peer_osd) != msgr.osd_peer_fds.end())
        {
            // really connected :)
            continue_ops();
        }
    };

    st_cli.tfd = tfd;
    st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_load_config_hook(cfg); };
    st_cli.on_change_osd_state_hook = [this](uint64_t peer_osd) { on_change_osd_state_hook(peer_osd); };
    st_cli.on_change_hook = [this](json11::Json::object & changes) { on_change_hook(changes); };
    st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };

    log_level = config["log_level"].int64_value();
    st_cli.parse_config(config);
    st_cli.load_global_config();
}

void cluster_client_t::continue_ops()
{
    for (auto op_it = unsent_ops.begin(); op_it != unsent_ops.end(); )
    {
        cluster_op_t *op = *op_it;
        if (op->needs_reslice && !op->sent_count)
        {
            op->parts.clear();
            op->done_count = 0;
            op->needs_reslice = false;
        }
        if (!op->parts.size())
        {
            unsent_ops.erase(op_it++);
            execute(op);
            continue;
        }
        if (!op->needs_reslice)
        {
            for (auto & op_part: op->parts)
            {
                if (!op_part.sent && !op_part.done)
                {
                    try_send(op, &op_part);
                }
            }
            if (op->sent_count == op->parts.size() - op->done_count)
            {
                unsent_ops.erase(op_it++);
                sent_ops.insert(op);
            }
            else
                op_it++;
        }
        else
            op_it++;
    }
}

static uint32_t is_power_of_two(uint64_t value)
{
    uint32_t l = 0;
    while (value > 1)
    {
        if (value & 1)
        {
            return 64;
        }
        value = value >> 1;
        l++;
    }
    return l;
}

void cluster_client_t::on_load_config_hook(json11::Json::object & config)
{
    bs_block_size = config["block_size"].uint64_value();
    bs_disk_alignment = config["disk_alignment"].uint64_value();
    bs_bitmap_granularity = config["bitmap_granularity"].uint64_value();
    if (!bs_block_size)
        bs_block_size = DEFAULT_BLOCK_SIZE;
    if (!bs_disk_alignment)
        bs_disk_alignment = DEFAULT_DISK_ALIGNMENT;
    if (!bs_bitmap_granularity)
        bs_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    {
        uint32_t block_order;
        if ((block_order = is_power_of_two(bs_block_size)) >= 64 || bs_block_size < MIN_BLOCK_SIZE || bs_block_size >= MAX_BLOCK_SIZE)
            throw std::runtime_error("Bad block size");
    }
    if (config.find("pg_stripe_size") != config.end())
    {
        pg_stripe_size = config["pg_stripe_size"].uint64_value();
        if (!pg_stripe_size)
            pg_stripe_size = DEFAULT_PG_STRIPE_SIZE;
    }
    msgr.peer_connect_interval = config["peer_connect_interval"].uint64_value();
    if (!msgr.peer_connect_interval)
        msgr.peer_connect_interval = DEFAULT_PEER_CONNECT_INTERVAL;
    msgr.peer_connect_timeout = config["peer_connect_timeout"].uint64_value();
    if (!msgr.peer_connect_timeout)
        msgr.peer_connect_timeout = DEFAULT_PEER_CONNECT_TIMEOUT;
}

void cluster_client_t::on_load_pgs_hook(bool success)
{
    if (success)
    {
        pg_count = st_cli.pg_config.size();
        continue_ops();
    }
}

void cluster_client_t::on_change_hook(json11::Json::object & changes)
{
    if (pg_count != st_cli.pg_config.size())
    {
        // At this point, all operations should be suspended
        // And they need to be resliced!
        for (auto op: unsent_ops)
        {
            op->needs_reslice = true;
        }
        for (auto op: sent_ops)
        {
            op->needs_reslice = true;
        }
        pg_count = st_cli.pg_config.size();
    }
    continue_ops();
}

void cluster_client_t::on_change_osd_state_hook(uint64_t peer_osd)
{
    if (msgr.wanted_peers.find(peer_osd) != msgr.wanted_peers.end())
    {
        msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
    }
}

void cluster_client_t::execute(cluster_op_t *op)
{
    if (op->opcode != OSD_OP_READ && op->opcode != OSD_OP_OUT || !op->inode || !op->len ||
        op->offset % bs_disk_alignment || op->len % bs_disk_alignment)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    if (!pg_stripe_size)
    {
        // Config is not loaded yet
        unsent_ops.insert(op);
        return;
    }
    // Slice the request into individual object stripe requests
    // Primary OSDs still operate individual stripes, but their size is multiplied by PG minsize in case of EC
    uint64_t pg_block_size = bs_block_size * pg_part_count;
    uint64_t first_stripe = (op->offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = ((op->offset + op->len + pg_block_size - 1) / pg_block_size - 1) * pg_block_size;
    int part_count = 0;
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        if (op->offset < (stripe+pg_block_size) && (op->offset+op->len) > stripe)
        {
            part_count++;
        }
    }
    op->parts.resize(part_count);
    bool resend = false;
    int i = 0;
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        uint64_t stripe_end = stripe + pg_block_size;
        if (op->offset < stripe_end && (op->offset+op->len) > stripe)
        {
            pg_num_t pg_num = (op->inode + stripe/pg_stripe_size) % pg_count + 1;
            op->parts[i] = {
                .parent = op,
                .offset = op->offset < stripe ? stripe : op->offset,
                .len = (uint32_t)((op->offset+op->len) > stripe_end ? pg_block_size : op->offset+op->len-stripe),
                .pg_num = pg_num,
                .buf = op->buf + (op->offset < stripe ? stripe-op->offset : 0),
                .sent = false,
                .done = false,
            };
            if (!try_send(op, &op->parts[i]))
            {
                // Part needs to be sent later
                resend = true;
            }
            i++;
        }
    }
    if (resend)
    {
        unsent_ops.insert(op);
    }
    else
    {
        sent_ops.insert(op);
    }
}

bool cluster_client_t::try_send(cluster_op_t *op, cluster_op_part_t *part)
{
    auto pg_it = st_cli.pg_config.find(part->pg_num);
    if (pg_it != st_cli.pg_config.end() &&
        !pg_it->second.pause && pg_it->second.cur_primary)
    {
        osd_num_t primary_osd = pg_it->second.cur_primary;
        auto peer_it = msgr.osd_peer_fds.find(primary_osd);
        if (peer_it != msgr.osd_peer_fds.end())
        {
            int peer_fd = peer_it->second;
            part->osd_num = primary_osd;
            part->sent = true;
            op->sent_count++;
            part->op = {
                .op_type = OSD_OP_OUT,
                .peer_fd = peer_fd,
                .req = { .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = op_id++,
                        .opcode = op->opcode,
                    },
                    .inode = op->inode,
                    .offset = part->offset,
                    .len = part->len,
                } },
                .callback = [this, part](osd_op_t *op_part)
                {
                    handle_op_part(part);
                },
            };
            part->op.send_list.push_back(part->op.req.buf, OSD_PACKET_SIZE);
            if (op->opcode == OSD_OP_WRITE)
            {
                part->op.send_list.push_back(part->buf, part->len);
            }
            else
            {
                part->op.buf = part->buf;
            }
            msgr.outbox_push(&part->op);
            return true;
        }
        else if (msgr.wanted_peers.find(primary_osd) == msgr.wanted_peers.end())
        {
            msgr.connect_peer(primary_osd, st_cli.peer_states[primary_osd]);
        }
    }
    return false;
}

void cluster_client_t::handle_op_part(cluster_op_part_t *part)
{
    cluster_op_t *op = part->parent;
    part->sent = false;
    op->sent_count--;
    part->op.buf = NULL;
    if (part->op.reply.hdr.retval != part->op.req.rw.len)
    {
        // Operation failed, retry
        printf(
            "Operation part failed on OSD %lu: retval=%ld (expected %u), reconnecting\n",
            part->osd_num, part->op.reply.hdr.retval, part->op.req.rw.len
        );
        msgr.stop_client(part->op.peer_fd);
        if (op->sent_count == op->parts.size() - op->done_count - 1)
        {
            // Resend later when OSDs come up
            // FIXME: Check for different types of errors
            // FIXME: Repeat operations after a small timeout, for the case when OSD is coming up
            sent_ops.erase(op);
            unsent_ops.insert(op);
        }
        if (op->sent_count == 0 && op->needs_reslice)
        {
            // PG count has changed, reslice the operation
            unsent_ops.erase(op);
            op->parts.clear();
            op->done_count = 0;
            op->needs_reslice = false;
            execute(op);
        }
    }
    else
    {
        // OK
        part->done = true;
        op->done_count++;
        if (op->done_count >= op->parts.size())
        {
            // Finished!
            op->retval = op->len;
            std::function<void(cluster_op_t*)>(op->callback)(op);
        }
    }
}
