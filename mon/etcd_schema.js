// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

// FIXME document all etcd keys and config variables in the form of JSON schema or similar
const etcd_nonempty_keys = {
    'config/global': 1,
    'config/node_placement': 1,
    'config/pools': 1,
    'config/pgs': 1,
    'history/last_clean_pgs': 1,
    'stats': 1,
};
const etcd_allow = new RegExp('^'+[
    'config/global',
    'config/node_placement',
    'config/pools',
    'config/osd/[1-9]\\d*',
    'config/pgs',
    'config/inode/[1-9]\\d*/[1-9]\\d*',
    'osd/state/[1-9]\\d*',
    'osd/stats/[1-9]\\d*',
    'osd/inodestats/[1-9]\\d*',
    'osd/space/[1-9]\\d*',
    'mon/master',
    'mon/member/[a-f0-9]+',
    'pg/state/[1-9]\\d*/[1-9]\\d*',
    'pg/stats/[1-9]\\d*/[1-9]\\d*',
    'pg/history/[1-9]\\d*/[1-9]\\d*',
    'history/last_clean_pgs',
    'inode/stats/[1-9]\\d*/\\d+',
    'pool/stats/[1-9]\\d*',
    'stats',
    'index/image/.*',
    'index/maxid/[1-9]\\d*',
].join('$|^')+'$');

const etcd_tree = {
    config: {
        /* global: {
            // WARNING: NOT ALL OF THESE ARE ACTUALLY CONFIGURABLE HERE
            // THIS IS JUST A POOR MAN'S CONFIG DOCUMENTATION
            // etcd connection
            config_path: "/etc/vitastor/vitastor.conf",
            etcd_prefix: "/vitastor",
            // etcd connection - configurable online
            etcd_address: "10.0.115.10:2379/v3",
            // mon
            etcd_mon_ttl: 5, // min: 1
            etcd_mon_timeout: 1000, // ms. min: 0
            etcd_mon_retries: 5, // min: 0
            mon_change_timeout: 1000, // ms. min: 100
            mon_retry_change_timeout: 50, // ms. min: 10
            mon_stats_timeout: 1000, // ms. min: 100
            osd_out_time: 600, // seconds. min: 0
            placement_levels: { datacenter: 1, rack: 2, host: 3, osd: 4, ... },
            use_old_pg_combinator: false,
            // client and osd
            tcp_header_buffer_size: 65536,
            use_sync_send_recv: false,
            use_rdma: true,
            rdma_device: null, // for example, "rocep5s0f0"
            rdma_port_num: 1,
            rdma_gid_index: 0,
            rdma_mtu: 4096,
            rdma_max_sge: 128,
            rdma_max_send: 8,
            rdma_max_recv: 16,
            rdma_max_msg: 132096,
            block_size: 131072,
            disk_alignment: 4096,
            bitmap_granularity: 4096,
            immediate_commit: 'all', // 'none', 'all' or 'small'
            // client - configurable online
            client_max_dirty_bytes: 33554432,
            client_max_dirty_ops: 1024,
            client_enable_writeback: false,
            client_max_buffered_bytes: 33554432,
            client_max_buffered_ops: 1024,
            client_max_writeback_iodepth: 256,
            client_retry_interval: 50, // ms. min: 10
            client_eio_retry_interval: 1000, // ms
            client_retry_enospc: true,
            osd_nearfull_ratio: 0.95,
            // client and osd - configurable online
            log_level: 0,
            peer_connect_interval: 5, // seconds. min: 1
            peer_connect_timeout: 5, // seconds. min: 1
            osd_idle_timeout: 5, // seconds. min: 1
            osd_ping_timeout: 5, // seconds. min: 1
            max_etcd_attempts: 5,
            etcd_quick_timeout: 1000, // ms
            etcd_slow_timeout: 5000, // ms
            etcd_keepalive_timeout: 30, // seconds, default is max(30, etcd_report_interval*2)
            etcd_ws_keepalive_interval: 5, // seconds
            // osd
            etcd_report_interval: 5, // seconds
            etcd_stats_interval: 30, // seconds
            run_primary: true,
            osd_network: null, // "192.168.7.0/24" or an array of masks
            bind_address: "0.0.0.0",
            bind_port: 0,
            readonly: false,
            osd_memlock: false,
            // osd - configurable online
            autosync_interval: 5,
            autosync_writes: 128,
            client_queue_depth: 128, // unused
            recovery_queue_depth: 1,
            recovery_sleep_us: 0,
            recovery_tune_util_low: 0.1,
            recovery_tune_client_util_low: 0,
            recovery_tune_util_high: 1.0,
            recovery_tune_client_util_high: 0.5,
            recovery_tune_interval: 1,
            recovery_tune_agg_interval: 10, // 10 times recovery_tune_interval
            recovery_tune_sleep_min_us: 10, // 10 microseconds
            recovery_pg_switch: 128,
            recovery_sync_batch: 16,
            no_recovery: false,
            no_rebalance: false,
            print_stats_interval: 3,
            slow_log_interval: 10,
            inode_vanish_time: 60,
            auto_scrub: false,
            no_scrub: false,
            scrub_interval: '30d', // 1s/1m/1h/1d
            scrub_queue_depth: 1,
            scrub_sleep: 0, // milliseconds
            scrub_list_limit: 1000, // objects to list on one scrub iteration
            scrub_find_best: true,
            scrub_ec_max_bruteforce: 100, // maximum EC error locator brute-force iterators
            // blockstore - fixed in superblock
            block_size,
            disk_alignment,
            journal_block_size,
            meta_block_size,
            bitmap_granularity,
            journal_device,
            journal_offset,
            journal_size,
            disable_journal_fsync,
            data_device,
            data_offset,
            data_size,
            disable_data_fsync,
            meta_device,
            meta_offset,
            disable_meta_fsync,
            disable_device_lock,
            // blockstore - configurable offline
            inmemory_metadata,
            inmemory_journal,
            journal_sector_buffer_count,
            journal_no_same_sector_overwrites,
            // blockstore - configurable online
            max_write_iodepth,
            min_flusher_count: 1,
            max_flusher_count: 256,
            throttle_small_writes: false,
            throttle_target_iops: 100,
            throttle_target_mbs: 100,
            throttle_target_parallelism: 1,
            throttle_threshold_us: 50,
        }, */
        global: {},
        /* node_placement: {
            host1: { level: 'host', parent: 'rack1' },
            ...
        }, */
        node_placement: {},
        /* pools: {
            <id>: {
                name: 'testpool',
                // 'ec' uses Reed-Solomon-Vandermonde codes, 'jerasure' is an alias for 'ec'
                scheme: 'replicated' | 'xor' | 'ec' | 'jerasure',
                pg_size: 3,
                pg_minsize: 2,
                // number of parity chunks, required for EC
                parity_chunks?: 1,
                pg_count: 100,
                // default is failure_domain=host
                failure_domain?: 'host',
                // additional failure domain rules; failure_domain=x is equivalent to x=123..N
                level_placement?: 'dc=112233 host=123456',
                raw_placement?: 'any, dc=1 host!=1, dc=1 host!=(1,2)',
                old_combinator: false,
                max_osd_combinations: 10000,
                // block_size, bitmap_granularity, immediate_commit must match all OSDs used in that pool
                block_size: 131072,
                bitmap_granularity: 4096,
                // 'all'/'small'/'none', same as in OSD options
                immediate_commit: 'all',
                pg_stripe_size: 0,
                root_node?: 'rack1',
                // restrict pool to OSDs having all of these tags
                osd_tags?: 'nvme' | [ 'nvme', ... ],
                // prefer to put primary on OSD with these tags
                primary_affinity_tags?: 'nvme' | [ 'nvme', ... ],
                // scrub interval
                scrub_interval?: '30d',
            },
            ...
        }, */
        pools: {},
        osd: {
            /* <id>: { reweight?: 1, tags?: [ 'nvme', ... ], noout?: true }, ... */
        },
        /* pgs: {
            hash: string,
            items: {
                <pool_id>: {
                    <pg_id>: {
                        osd_set: [ 1, 2, 3 ],
                        primary: 1,
                        pause: false,
                    }
                }
            }
        }, */
        pgs: {},
        /* inode: {
            <pool_id>: {
                <inode_t>: {
                    name: string,
                    size?: uint64_t, // bytes
                    parent_pool?: <pool_id>,
                    parent_id?: <inode_t>,
                    readonly?: boolean,
                }
            }
        }, */
        inode: {},
    },
    osd: {
        state: {
            /* <osd_num_t>: {
                state: "up",
                addresses: string[],
                host: string,
                port: uint16_t,
                primary_enabled: boolean,
                blockstore_enabled: boolean,
            }, */
        },
        stats: {
            /* <osd_num_t>: {
                time: number, // unix time
                data_block_size: uint64_t, // bytes
                bitmap_granularity: uint64_t, // bytes
                immediate_commit: "all"|"small"|"none",
                blockstore_ready: boolean,
                size: uint64_t, // bytes
                free: uint64_t, // bytes
                host: string,
                op_stats: {
                    <string>: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                },
                subop_stats: {
                    <string>: { count: uint64_t, usec: uint64_t },
                },
                recovery_stats: {
                    degraded: { count: uint64_t, bytes: uint64_t },
                    misplaced: { count: uint64_t, bytes: uint64_t },
                },
            }, */
        },
        inodestats: {
            /* <pool_id>: {
                <inode_t>: {
                    read: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    write: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                },
            }, */
        },
        space: {
            /* <osd_num_t>: {
                <pool_id>: {
                    <inode_t>: uint64_t, // bytes
                },
            }, */
        },
    },
    mon: {
        master: {
            /* ip: [ string ], id: uint64_t */
        },
        member: {
            /* <uint64_t>: { ip: [ string ] }, */
        },
    },
    pg: {
        state: {
            /* <pool_id>: {
                <pg_id>: {
                    primary: osd_num_t,
                    state: ("starting"|"peering"|"incomplete"|"active"|"repeering"|"stopping"|"offline"|
                        "degraded"|"has_incomplete"|"has_degraded"|"has_misplaced"|"has_unclean"|
                        "has_invalid"|"has_inconsistent"|"has_corrupted"|"left_on_dead"|"scrubbing")[],
                }
            }, */
        },
        stats: {
            /* <pool_id>: {
                <pg_id>: {
                    object_count: uint64_t,
                    clean_count: uint64_t,
                    misplaced_count: uint64_t,
                    degraded_count: uint64_t,
                    incomplete_count: uint64_t,
                    write_osd_set: osd_num_t[],
                },
            }, */
        },
        history: {
            /* <pool_id>: {
                <pg_id>: {
                    osd_sets: osd_num_t[][],
                    all_peers: osd_num_t[],
                    epoch: uint64_t,
                    next_scrub: uint64_t,
                },
            }, */
        },
    },
    inode: {
        stats: {
            /* <pool_id>: {
                <inode_t>: {
                    raw_used: uint64_t, // raw used bytes on OSDs
                    read: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
                    write: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
                    delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
                },
            }, */
        },
    },
    pool: {
        stats: {
            /* <pool_id>: {
                used_raw_tb: float, // used raw space in the pool
                total_raw_tb: float, // maximum amount of space in the pool
                raw_to_usable: float, // raw to usable ratio
                space_efficiency: float, // 0..1
            } */
        },
    },
    stats: {
        /* op_stats: {
            <string>: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
        },
        subop_stats: {
            <string>: { count: uint64_t, usec: uint64_t, iops: uint64_t, lat: uint64_t },
        },
        recovery_stats: {
            degraded: { count: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t },
            misplaced: { count: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t },
        },
        object_counts: {
            object: uint64_t,
            clean: uint64_t,
            misplaced: uint64_t,
            degraded: uint64_t,
            incomplete: uint64_t,
        },
        object_bytes: {
            total: uint64_t,
            clean: uint64_t,
            misplaced: uint64_t,
            degraded: uint64_t,
            incomplete: uint64_t,
        }, */
    },
    history: {
        last_clean_pgs: {},
    },
    index: {
        image: {
            /* <name>: {
                id: uint64_t,
                pool_id: uint64_t,
            }, */
        },
        maxid: {
            /* <pool_id>: uint64_t, */
        },
    },
};

module.exports = {
    etcd_nonempty_keys,
    etcd_allow,
    etcd_tree,
};
