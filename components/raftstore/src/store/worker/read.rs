// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam::TrySendError;
use kvproto::errorpb;
use kvproto::metapb;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse};
use time::Timespec;

use crate::errors::RAFTSTORE_IS_BUSY;
use crate::store::fsm::RaftRouter;
use crate::store::util::{self, LeaseState, RemoteLease};
use crate::store::{
    cmd_resp, Peer, ProposalRouter, RaftCommand, ReadExecutor, ReadResponse, RegionSnapshot,
    RequestInspector, RequestPolicy,
};
use crate::Result;
use engine_traits::KvEngine;
use tikv_util::collections::HashMap;
use tikv_util::threadpool::ThreadReadId;
use tikv_util::time::monotonic_raw_now;
use tikv_util::time::Instant;

use super::metrics::*;
use crate::store::fsm::store::StoreMeta;

/// A read only delegate of `Peer`.
#[derive(Clone, Debug)]
pub struct ReadDelegate {
    region: Arc<metapb::Region>,
    peer_id: u64,
    term: u64,
    applied_index_term: u64,
    leader_lease: Option<RemoteLease>,
    last_valid_ts: Timespec,

    tag: String,
    invalid: Arc<AtomicBool>,
    pub(crate) rejected_by_appiled_term: i64,
    pub(crate) rejected_by_no_lease: i64,
}

impl ReadDelegate {
    pub fn from_peer(peer: &Peer) -> ReadDelegate {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        ReadDelegate {
            region: Arc::new(region),
            peer_id,
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term(),
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, peer_id),
            invalid: Arc::new(AtomicBool::new(false)),
            rejected_by_appiled_term: 0,
            rejected_by_no_lease: 0,
        }
    }

    pub fn mark_invalid(&self) {
        self.invalid.store(true, Ordering::Release);
    }

    pub fn fresh_valid_ts(&mut self) {
        self.last_valid_ts = monotonic_raw_now();
        // We add one second to make sure that snapshot-cache sent by other thread is really created after this region has applied to current term exactly
        self.last_valid_ts.sec += 1;
    }

    pub fn update(&mut self, progress: Progress) {
        match progress {
            Progress::Region(region) => {
                self.region = Arc::new(region);
            }
            Progress::Term(term) => {
                self.fresh_valid_ts();
                self.term = term;
            }
            Progress::AppliedIndexTerm(applied_index_term) => {
                self.fresh_valid_ts();
                self.applied_index_term = applied_index_term;
            }
            Progress::LeaderLease(leader_lease) => {
                self.fresh_valid_ts();
                self.leader_lease = Some(leader_lease);
            }
        }
    }

    // TODO: return ReadResponse once we remove batch snapshot.
    fn is_in_leader_lease(&self, ts: Timespec, metrics: &mut ReadMetrics) -> bool {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    return true;
                } else {
                    metrics.rejected_by_lease_expire += 1;
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                metrics.rejected_by_term_mismatch += 1;
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        false
    }
}

impl Display for ReadDelegate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.region.get_id(),
            self.peer_id,
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub enum Progress {
    Region(metapb::Region),
    Term(u64),
    AppliedIndexTerm(u64),
    LeaderLease(RemoteLease),
}

impl Progress {
    pub fn region(region: metapb::Region) -> Progress {
        Progress::Region(region)
    }

    pub fn term(term: u64) -> Progress {
        Progress::Term(term)
    }

    pub fn applied_index_term(applied_index_term: u64) -> Progress {
        Progress::AppliedIndexTerm(applied_index_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }
}

pub struct LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot>,
    E: KvEngine,
{
    store_id: Cell<Option<u64>>,
    store_meta: Arc<Mutex<StoreMeta>>,
    kv_engine: E,
    metrics: ReadMetrics,
    // region id -> ReadDelegate
    delegates: HashMap<u64, Option<ReadDelegate>>,
    // A channel to raftstore.
    snap_cache: Option<RegionSnapshot<E::Snapshot>>,
    cache_read_id: ThreadReadId,
    router: C,
    tag: String,
}

impl<E: KvEngine> LocalReader<RaftRouter<E>, E> {
    pub fn new(kv_engine: E, store_meta: Arc<Mutex<StoreMeta>>, router: RaftRouter<E>) -> Self {
        let cache_read_id = ThreadReadId::new();
        LocalReader {
            store_meta,
            kv_engine,
            router,
            snap_cache: None,
            cache_read_id,
            store_id: Cell::new(None),
            metrics: Default::default(),
            delegates: HashMap::default(),
            tag: "[local_reader]".to_string(),
        }
    }
}

impl<C, E> LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot>,
    E: KvEngine,
{
    fn redirect(&mut self, mut cmd: RaftCommand<E::Snapshot>) {
        debug!("localreader redirects command"; "tag" => &self.tag, "command" => ?cmd);
        let region_id = cmd.request.get_header().get_region_id();
        let mut err = errorpb::Error::default();
        match self.router.send(cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(c)) => {
                self.metrics.rejected_by_channel_full += 1;
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrySendError::Disconnected(c)) => {
                self.metrics.rejected_by_no_region += 1;
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                cmd = c;
            }
        }

        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
        };

        cmd.callback.invoke_read(read_resp);
    }

    fn pre_propose_raft_command(&mut self, req: &RaftCmdRequest) -> Result<Option<ReadDelegate>> {
        // Check store id.
        if self.store_id.get().is_none() {
            let store_id = self.store_meta.lock().unwrap().store_id;
            self.store_id.set(store_id);
        }
        let store_id = self.store_id.get().unwrap();

        if let Err(e) = util::check_store_id(req, store_id) {
            self.metrics.rejected_by_store_id_mismatch += 1;
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let mut delegate = match self.delegates.get_mut(&region_id) {
            Some(delegate) => {
                fail_point!("localreader_on_find_delegate");
                match delegate.take() {
                    Some(d) => d,
                    None => return Ok(None),
                }
            }
            None => {
                self.metrics.rejected_by_cache_miss += 1;
                debug!("rejected by cache miss"; "region_id" => region_id);
                return Ok(None);
            }
        };

        if delegate.invalid.load(Ordering::Acquire) {
            self.delegates.remove(&region_id);
            return Ok(None);
        }

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.peer_id) {
            self.metrics.rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.term) {
            debug!(
                "check term";
                "delegate_term" => delegate.term,
                "header_term" => req.get_header().get_term(),
            );
            self.metrics.rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.region, false).is_err() {
            self.metrics.rejected_by_epoch += 1;
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.tag);
            return Ok(None);
        }

        match delegate.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some(delegate)),
            // It can not handle other policies.
            Ok(_) => {
                self.metrics.rejected_by_epoch += delegate.rejected_by_appiled_term;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    // It can only handle read command.
    pub fn propose_raft_command(
        &mut self,
        read_id: Option<ThreadReadId>,
        cmd: RaftCommand<E::Snapshot>,
    ) {
        let region_id = cmd.request.get_header().get_region_id();
        loop {
            match self.pre_propose_raft_command(&cmd.request) {
                Ok(Some(delegate)) => {
                    let use_cache_snapshot = read_id
                        .as_ref()
                        .map_or(false, |id| *id == self.cache_read_id)
                        && self.snap_cache.as_ref().map_or(false, |snap_cache| {
                            snap_cache.get_ts() > delegate.last_valid_ts
                        });
                    let snapshot_ts = if use_cache_snapshot {
                        self.snap_cache.as_ref().unwrap().get_ts()
                    } else {
                        monotonic_raw_now()
                    };
                    if delegate.is_in_leader_lease(snapshot_ts.clone(), &mut self.metrics) {
                        // Cache snapshot_time for remaining requests in the same batch.
                        let (mut response, need_snapshot) = ReadExecutor::<E>::execute(
                            &self.kv_engine,
                            &cmd.request,
                            delegate.region.as_ref(),
                            None,
                        );
                        let snapshot = if need_snapshot {
                            if use_cache_snapshot {
                                self.metrics.local_executed_cache_requests += 1;
                                self.snap_cache.as_ref().map(|snap| {
                                    let mut snapshot = snap.clone();
                                    snapshot.set_region(delegate.region.clone());
                                    snapshot
                                })
                            } else {
                                let snap = RegionSnapshot::from_snapshot(
                                    Arc::new(self.kv_engine.snapshot()),
                                    delegate.region.clone(),
                                    snapshot_ts,
                                );
                                if let Some(id) = read_id {
                                    self.snap_cache = Some(snap.clone());
                                    self.cache_read_id = id;
                                }
                                self.metrics.local_executed_requests += 1;
                                Some(snap)
                            }
                        } else {
                            self.metrics.local_executed_requests += 1;
                            None
                        };
                        // Leader can read local if and only if it is in lease.
                        cmd_resp::bind_term(&mut response, delegate.term);
                        cmd.callback
                            .invoke_read(ReadResponse { response, snapshot });
                        self.delegates.insert(region_id, Some(delegate));
                        return;
                    }
                    break;
                }
                // It can not handle the request, forwards to raftstore.
                Ok(None) => {
                    if self.delegates.get(&region_id).is_some() {
                        break;
                    }
                    let meta = self.store_meta.lock().unwrap();
                    match meta.readers.get(&region_id).cloned() {
                        Some(reader) => {
                            self.delegates.insert(region_id, Some(reader));
                        }
                        None => {
                            self.metrics.rejected_by_no_region += 1;
                            debug!("rejected by no region"; "region_id" => region_id);
                            break;
                        }
                    }
                }
                Err(e) => {
                    let mut response = cmd_resp::new_error(e);
                    if let Some(Some(ref delegate)) = self.delegates.get(&region_id) {
                        cmd_resp::bind_term(&mut response, delegate.term);
                    }
                    cmd.callback.invoke_read(ReadResponse {
                        response,
                        snapshot: None,
                    });
                    self.delegates.remove(&region_id);
                    return;
                }
            }
        }
        // Remove delegate for updating it by next cmd execution.
        self.delegates.remove(&region_id);
        // Forward to raftstore.
        self.redirect(cmd);
    }

    #[inline]
    pub fn read(&mut self, read_id: Option<ThreadReadId>, cmd: RaftCommand<E::Snapshot>) {
        self.propose_raft_command(read_id, cmd);
        self.metrics.maybe_flush();
    }

    pub fn release_snapshot_cache(&mut self) {
        self.snap_cache.take();
    }

    /// Task accepts `RaftCmdRequest`s that contain Get/Snap requests.
    /// Returns `true`, it can be safely sent to localreader,
    /// Returns `false`, it must not be sent to localreader.
    #[inline]
    pub fn acceptable(request: &RaftCmdRequest) -> bool {
        if request.has_admin_request() || request.has_status_request() {
            false
        } else {
            for r in request.get_requests() {
                match r.get_cmd_type() {
                    CmdType::Get | CmdType::Snap => (),
                    CmdType::Delete
                    | CmdType::Put
                    | CmdType::DeleteRange
                    | CmdType::Prewrite
                    | CmdType::IngestSst
                    | CmdType::ReadIndex
                    | CmdType::Invalid => return false,
                }
            }
            true
        }
    }
}

impl<C, E> Clone for LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot> + Clone,
    E: KvEngine,
{
    fn clone(&self) -> Self {
        LocalReader {
            store_meta: self.store_meta.clone(),
            kv_engine: self.kv_engine.clone(),
            router: self.router.clone(),
            store_id: self.store_id.clone(),
            metrics: Default::default(),
            delegates: HashMap::default(),
            tag: self.tag.clone(),
            snap_cache: self.snap_cache.clone(),
            cache_read_id: ThreadReadId::new(),
        }
    }
}

impl RequestInspector for ReadDelegate {
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.applied_index_term == self.term {
            true
        } else {
            debug!(
                "rejected by term check";
                "tag" => &self.tag,
                "applied_index_term" => self.applied_index_term,
                "delegate_term" => ?self.term,
            );

            // only for metric.
            self.rejected_by_appiled_term += 1;
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if self.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!("rejected by leader lease"; "tag" => &self.tag);
            self.rejected_by_no_lease += 1;
            LeaseState::Expired
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 15_000; // 15s

#[derive(Clone)]
struct ReadMetrics {
    local_executed_requests: i64,
    local_executed_cache_requests: i64,
    // TODO: record rejected_by_read_quorum.
    rejected_by_store_id_mismatch: i64,
    rejected_by_peer_id_mismatch: i64,
    rejected_by_term_mismatch: i64,
    rejected_by_lease_expire: i64,
    rejected_by_no_region: i64,
    rejected_by_no_lease: i64,
    rejected_by_epoch: i64,
    rejected_by_appiled_term: i64,
    rejected_by_channel_full: i64,
    rejected_by_cache_miss: i64,

    last_flush_time: Instant,
}

impl Default for ReadMetrics {
    fn default() -> ReadMetrics {
        ReadMetrics {
            local_executed_requests: 0,
            local_executed_cache_requests: 0,
            rejected_by_store_id_mismatch: 0,
            rejected_by_peer_id_mismatch: 0,
            rejected_by_term_mismatch: 0,
            rejected_by_lease_expire: 0,
            rejected_by_no_region: 0,
            rejected_by_no_lease: 0,
            rejected_by_epoch: 0,
            rejected_by_appiled_term: 0,
            rejected_by_channel_full: 0,
            rejected_by_cache_miss: 0,
            last_flush_time: Instant::now(),
        }
    }
}

impl ReadMetrics {
    pub fn maybe_flush(&mut self) {
        if self.last_flush_time.elapsed() >= Duration::from_millis(METRICS_FLUSH_INTERVAL) {
            self.flush();
            self.last_flush_time = Instant::now();
        }
    }

    fn flush(&mut self) {
        if self.rejected_by_store_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .store_id_mismatch
                .inc_by(self.rejected_by_store_id_mismatch);
            self.rejected_by_store_id_mismatch = 0;
        }
        if self.rejected_by_peer_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .peer_id_mismatch
                .inc_by(self.rejected_by_peer_id_mismatch);
            self.rejected_by_peer_id_mismatch = 0;
        }
        if self.rejected_by_term_mismatch > 0 {
            LOCAL_READ_REJECT
                .term_mismatch
                .inc_by(self.rejected_by_term_mismatch);
            self.rejected_by_term_mismatch = 0;
        }
        if self.rejected_by_lease_expire > 0 {
            LOCAL_READ_REJECT
                .lease_expire
                .inc_by(self.rejected_by_lease_expire);
            self.rejected_by_lease_expire = 0;
        }
        if self.rejected_by_no_region > 0 {
            LOCAL_READ_REJECT
                .no_region
                .inc_by(self.rejected_by_no_region);
            self.rejected_by_no_region = 0;
        }
        if self.rejected_by_no_lease > 0 {
            LOCAL_READ_REJECT.no_lease.inc_by(self.rejected_by_no_lease);
            self.rejected_by_no_lease = 0;
        }
        if self.rejected_by_epoch > 0 {
            LOCAL_READ_REJECT.epoch.inc_by(self.rejected_by_epoch);
            self.rejected_by_epoch = 0;
        }
        if self.rejected_by_appiled_term > 0 {
            LOCAL_READ_REJECT
                .appiled_term
                .inc_by(self.rejected_by_appiled_term);
            self.rejected_by_appiled_term = 0;
        }
        if self.rejected_by_channel_full > 0 {
            LOCAL_READ_REJECT
                .channel_full
                .inc_by(self.rejected_by_channel_full);
            self.rejected_by_channel_full = 0;
        }
        if self.local_executed_cache_requests > 0 {
            LOCAL_READ_EXECUTED_CACHE_REQUESTS.inc_by(self.local_executed_cache_requests);
            self.local_executed_cache_requests = 0;
        }
        if self.local_executed_requests > 0 {
            LOCAL_READ_EXECUTED_REQUESTS.inc_by(self.local_executed_requests);
            self.local_executed_requests = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::*;
    use std::thread;

    use kvproto::raft_cmdpb::*;
    use tempfile::{Builder, TempDir};
    use time::Duration;

    use crate::store::util::Lease;
    use crate::store::Callback;
    use engine::rocks;
    use engine_rocks::{RocksEngine, RocksSnapshot};
    use engine_traits::ALL_CFS;
    use tikv_util::time::monotonic_raw_now;

    use super::*;

    #[allow(clippy::type_complexity)]
    fn new_reader(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) -> (
        TempDir,
        LocalReader<SyncSender<RaftCommand<RocksSnapshot>>, RocksEngine>,
        Receiver<RaftCommand<RocksSnapshot>>,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let db =
            rocks::util::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let (ch, rx) = sync_channel(1);
        let reader = LocalReader {
            store_meta,
            store_id: Cell::new(Some(store_id)),
            router: ch,
            kv_engine: RocksEngine::from_db(Arc::new(db)),
            delegates: HashMap::default(),
            last_read_ts: monotonic_raw_now(),
            metrics: Default::default(),
            tag: "foo".to_owned(),
        };
        (path, reader, rx)
    }

    fn new_peers(store_id: u64, pr_ids: Vec<u64>) -> Vec<metapb::Peer> {
        pr_ids
            .into_iter()
            .map(|id| {
                let mut pr = metapb::Peer::default();
                pr.set_store_id(store_id);
                pr.set_id(id);
                pr
            })
            .collect()
    }

    fn must_redirect(
        reader: &mut LocalReader<SyncSender<RaftCommand<RocksSnapshot>>, RocksEngine>,
        rx: &Receiver<RaftCommand<RocksSnapshot>>,
        cmd: RaftCmdRequest,
    ) {
        let task = RaftCommand::new(
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        reader.propose_raft_command(None, task);
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd
        );
    }

    fn must_not_redirect(
        reader: &mut LocalReader<SyncSender<RaftCommand<RocksSnapshot>>, RocksEngine>,
        rx: &Receiver<RaftCommand<RocksSnapshot>>,
        task: RaftCommand<RocksSnapshot>,
    ) {
        reader.propose_raft_command(None, task);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_read() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());

        // region: 1,
        // peers: 2, 3, 4,
        // leader:2,
        // from "" to "",
        // epoch 1, 1,
        // term 6.
        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![2, 3, 4]);
        region1.set_peers(prs.clone().into());
        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let leader2 = prs[0].clone();
        region1.set_region_epoch(epoch13.clone());
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(1)); // 1s is long enough.

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader2.clone());
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The region is not register yet.
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_no_region, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 1);

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        // But the applied_index_term is stale.
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: region1.clone(),
                peer_id: leader2.get_id(),
                term: term6,
                applied_index_term: term6 - 1,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                invalid: Arc::new(AtomicBool::new(false)),
            };
            meta.readers.insert(1, read_delegate);
        }

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_cache_miss, 2);
        assert_eq!(reader.metrics.rejected_by_appiled_term, 1);
        assert!(reader.delegates.get(&1).is_none());

        // Make the applied_index_term matches current term.
        let pg = Progress::applied_index_term(term6);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<RocksSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // Let's read.
        let region = region1;
        let task = RaftCommand::<RocksSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region);
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        // Wait for expiration.
        thread::sleep(Duration::seconds(1).to_std().unwrap());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_lease_expire, 1);

        // Renew lease.
        lease.renew(monotonic_raw_now());

        // Store id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        let task = RaftCommand::<RocksSnapshot>::new(
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        );
        reader.propose_raft_command(None, task);
        assert_eq!(reader.metrics.rejected_by_store_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // metapb::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        let task = RaftCommand::<RocksSnapshot>::new(
            cmd_peer_id,
            Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                assert!(
                    resp.response.get_header().has_error(),
                    "{:?}",
                    resp.response
                );
                assert!(resp.snapshot.is_none());
            })),
        );
        reader.propose_raft_command(None, task);
        assert_eq!(reader.metrics.rejected_by_peer_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 4);

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 5);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        let task = RaftCommand::<RocksSnapshot>::new(
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        reader.propose_raft_command(None, task);
        assert_eq!(reader.metrics.rejected_by_term_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 6);

        // Stale epoch.
        let mut epoch12 = epoch13;
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.rejected_by_epoch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 7);

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection = reader.metrics.rejected_by_lease_expire;
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            reader.metrics.rejected_by_lease_expire,
            previous_lease_rejection + 1
        );
        assert_eq!(reader.metrics.rejected_by_cache_miss, 8);

        // Channel full.
        let task1 = RaftCommand::<RocksSnapshot>::new(cmd.clone(), Callback::None);
        let task_full = RaftCommand::<RocksSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        reader.propose_raft_command(task1);
        reader.propose_raft_command(task_full);
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(reader.metrics.rejected_by_channel_full, 1);

        // Reject by term mismatch in lease.
        let previous_term_rejection = reader.metrics.rejected_by_term_mismatch;
        let mut cmd9 = cmd;
        cmd9.mut_header().set_term(term6 + 3);
        let task = RaftCommand::<RocksSnapshot>::new(
            cmd9.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::term(term6 + 3));
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_index_term(term6 + 3));
        }
        reader.propose_raft_command(None, task);
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd9
        );
        assert_eq!(
            reader.metrics.rejected_by_term_mismatch,
            previous_term_rejection + 1,
        );
    }
}
