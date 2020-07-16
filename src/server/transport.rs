// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use std::sync::{Arc, RwLock};

use super::{Config, Result};
use crate::server::metrics::*;
use crate::server::raft_client::{Conn, RaftClient};
use crate::server::resolve::StoreAddrResolver;
use crate::server::snap::Task as SnapTask;
use crossbeam::channel::SendError;
use engine_rocks::RocksSnapshot;
use raft::SnapshotStatus;
use raftstore::router::RaftStoreRouter;
use raftstore::store::Transport;
use raftstore::Result as RaftStoreResult;
use tikv_util::collections::HashSet;
use tikv_util::collections::{HashMap, HashMapEntry};
use tikv_util::worker::Scheduler;
use tikv_util::HandyRwLock;

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksSnapshot> + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: Arc<RwLock<RaftClient<T>>>,
    snap_scheduler: Scheduler<SnapTask>,
    pub raft_router: T,
    resolving: Arc<RwLock<HashSet<u64>>>,
    conns: HashMap<(u64, u64), Conn>,
    cfg: Arc<Config>,
    resolver: S,
    need_flush_raft_client: bool,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksSnapshot> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: Arc::clone(&self.raft_client),
            snap_scheduler: self.snap_scheduler.clone(),
            raft_router: self.raft_router.clone(),
            resolving: Arc::clone(&self.resolving),
            resolver: self.resolver.clone(),
            // do not clone connection caches
            conns: HashMap::default(),
            cfg: self.cfg.clone(),
            need_flush_raft_client: self.need_flush_raft_client,
        }
    }
}

impl<T: RaftStoreRouter<RocksSnapshot> + 'static, S: StoreAddrResolver + 'static>
    ServerTransport<T, S>
{
    pub fn new(
        raft_client: Arc<RwLock<RaftClient<T>>>,
        snap_scheduler: Scheduler<SnapTask>,
        raft_router: T,
        resolver: S,
    ) -> ServerTransport<T, S> {
        let cfg = raft_client.rl().cfg.clone();
        ServerTransport {
            raft_client,
            snap_scheduler,
            raft_router,
            resolving: Arc::new(RwLock::new(Default::default())),
            conns: HashMap::default(),
            cfg,
            resolver,
            need_flush_raft_client: false,
        }
    }

    fn send_store(&mut self, store_id: u64, msg: RaftMessage) {
        // Wrapping the fail point in a closure, so we can modify
        // local variables without return,
        let transport_on_send_store_fp = || {
            fail_point!("transport_on_send_store", |sid| if let Some(sid) = sid {
                let sid: u64 = sid.parse().unwrap();
                if sid == store_id {
                    self.raft_client.wl().addrs.remove(&store_id);
                }
            })
        };
        transport_on_send_store_fp();
        // check the corresponding token for store.
        if msg.get_message().has_snapshot() {
            // TODO: avoid clone
            let addr = self.raft_client.rl().addrs.get(&store_id).cloned();
            if let Some(addr) = addr {
                return self.send_snapshot_sock(&addr, msg);
            }
        } else {
            let region_id = msg.region_id;
            if let Some(conn) = self.get_conn(store_id, region_id) {
                if let Err(SendError(msg)) = conn.stream.send(msg) {
                    drop(conn);
                    self.conns.remove(&(store_id, region_id));
                    let mut cli = self.raft_client.wl();
                    if let Some(addr) = cli.addrs.get(&store_id).cloned() {
                        let _ = cli.send(store_id, &addr, msg);
                        self.need_flush_raft_client = true;
                    }
                }
                return;
            }
        }

        // No connection, try to resolve it.
        if self.resolving.rl().contains(&store_id) {
            RESOLVE_STORE_COUNTER_STATIC.resolving.inc();
            // If we are resolving the address, drop the message here.
            debug!(
                "store address is being resolved, msg dropped";
                "store_id" => store_id,
                "message" => ?msg
            );
            self.report_unreachable(msg);
            return;
        }

        debug!("begin to resolve store address"; "store_id" => store_id);
        RESOLVE_STORE_COUNTER_STATIC.resolve.inc();
        self.resolving.wl().insert(store_id);
        self.resolve(store_id, msg);
    }

    // TODO: remove allow unused mut.
    // Compiler warns `mut addr ` and `mut transport_on_resolve_fp`, when we disable
    // the `failpoints` feature.
    #[allow(unused_mut)]
    fn resolve(&mut self, store_id: u64, msg: RaftMessage) {
        let trans = self.clone();
        let msg1 = msg.clone();
        let cb = Box::new(move |mut addr: Result<String>| {
            {
                // Wrapping the fail point in a closure, so we can modify
                // local variables without return.
                let mut transport_on_resolve_fp = || {
                    fail_point!(
                        "transport_snapshot_on_resolve",
                        msg.get_message().get_msg_type() == MessageType::MsgSnapshot,
                        |sid| if let Some(sid) = sid {
                            use std::mem;
                            let sid: u64 = sid.parse().unwrap();
                            if sid == store_id {
                                mem::swap(&mut addr, &mut Err(box_err!("injected failure")));
                            }
                        }
                    )
                };
                transport_on_resolve_fp();
            }

            // clear resolving.
            trans.resolving.wl().remove(&store_id);
            if let Err(e) = addr {
                RESOLVE_STORE_COUNTER_STATIC.failed.inc();
                error!("resolve store address failed"; "store_id" => store_id, "err" => ?e);
                trans.report_unreachable(msg);
                return;
            }

            RESOLVE_STORE_COUNTER_STATIC.success.inc();

            let addr = addr.unwrap();
            info!("resolve store address ok"; "store_id" => store_id, "addr" => %addr);
            // Send message without cache to make sure that the connection is created.
            {
                let mut guard = trans.raft_client.wl();
                guard.addrs.insert(store_id, addr.clone());
                if msg.get_message().has_snapshot() {
                    trans.send_snapshot_sock(&addr, msg);
                } else {
                    if guard.send(store_id, &addr, msg).is_err() {
                        error!("reconnect failed"; "store_id" => store_id, "address" => addr);
                    }
                }
                // There may be no messages in the near future, so flush it immediately.
                guard.flush();
            }
        });
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("resolve store address failed"; "store_id" => store_id, "err" => ?e);
            self.resolving.wl().remove(&store_id);
            RESOLVE_STORE_COUNTER_STATIC.failed.inc();
            self.report_unreachable(msg1);
        }
    }

    fn get_conn(&mut self, store_id: u64, region_id: u64) -> Option<&mut Conn> {
        let index = region_id % self.cfg.grpc_raft_conn_num as u64;
        match self.conns.entry((store_id, index)) {
            HashMapEntry::Occupied(e) => Some(e.into_mut()),
            HashMapEntry::Vacant(e) => {
                let mut cli = self.raft_client.wl();
                let addr = cli.addrs.get(&store_id).cloned();
                if let Some(addr) = addr {
                    let conn = cli.get_conn(&addr, region_id, store_id).clone();
                    Some(e.insert(conn))
                } else {
                    None
                }
            }
        }
    }

    fn send_snapshot_sock(&self, addr: &str, msg: RaftMessage) {
        let rep = self.new_snapshot_reporter(&msg);
        let cb = Box::new(move |res: Result<()>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        });
        if let Err(e) = self.snap_scheduler.schedule(SnapTask::Send {
            addr: addr.to_owned(),
            msg,
            cb,
        }) {
            if let SnapTask::Send { cb, .. } = e.into_inner() {
                error!(
                    "channel is unavailable, failed to schedule snapshot";
                    "to_addr" => addr
                );
                cb(Err(box_err!("failed to schedule snapshot")));
            }
        }
    }

    fn new_snapshot_reporter(&self, msg: &RaftMessage) -> SnapshotReporter<T> {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        SnapshotReporter {
            raft_router: self.raft_router.clone(),
            region_id,
            to_peer_id,
            to_store_id,
        }
    }

    pub fn report_unreachable(&self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let store_id = msg.get_to_peer().get_store_id();

        // Report snapshot failure.
        if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
            self.new_snapshot_reporter(&msg)
                .report(SnapshotStatus::Failure);
        }

        if let Err(e) = self.raft_router.report_unreachable(region_id, to_peer_id) {
            error!(
                "report peer unreachable failed";
                "region_id" => region_id,
                "to_store_id" => store_id,
                "to_peer_id" => to_peer_id,
                "err" => ?e
            );
        }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksSnapshot> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        self.send_store(to_store_id, msg);
        Ok(())
    }

    fn flush(&mut self) {
        let mut counter = 0;
        for conn in self.conns.values_mut() {
            if conn.stream.is_empty() {
                continue;
            }
            if let Some(notifier) = conn.stream.get_notifier() {
                notifier.notify();
                counter += 1;
            }
        }
        if self.need_flush_raft_client {
            self.raft_client.wl().flush();
        }
        RAFT_MESSAGE_FLUSH_COUNTER.inc_by(i64::from(counter));
    }
}

struct SnapshotReporter<T: RaftStoreRouter<RocksSnapshot> + 'static> {
    raft_router: T,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: RaftStoreRouter<RocksSnapshot> + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        debug!(
            "send snapshot";
            "to_peer_id" => self.to_peer_id,
            "region_id" => self.region_id,
            "status" => ?status
        );

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["snapshot", &*store])
                .inc();
        };

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.region_id, self.to_peer_id, status)
        {
            error!(
                "report snapshot to peer failes";
                "to_peer_id" => self.to_peer_id,
                "to_store_id" => self.to_store_id,
                "region_id" => self.region_id,
                "err" => ?e
            );
        }
    }
}
