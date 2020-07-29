// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::ClusterVersion;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use engine_rocks::RocksEngine;
use engine_traits::{CF_WRITE, TablePropertiesExt};
use tikv_util::worker::{FutureScheduler, Scheduler, Runnable};
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use raftstore::coprocessor::RegionInfoProvider;
use raftstore::store::util::find_peer;

use super::config::GcWorkerConfigManager;
use super::gc_worker::{GcSafePointProvider, GcTask};
use super::{is_compaction_filter_allowd};
use crate::storage::mvcc::check_need_gc;

const POLL_SAFE_POINT_INTERVAL_SECS: u64 = 60;

const BEGIN_KEY: &[u8] = b"";

const PROCESS_TYPE_GC: &str = "gc";
const PROCESS_TYPE_SCAN: &str = "scan";

/// The configurations of automatic GC.
pub struct AutoGcConfig<S: GcSafePointProvider, R: RegionInfoProvider> {
    pub safe_point_provider: S,
    pub region_info_provider: R,

    /// Used to find which peer of a region is on this TiKV, so that we can compose a `Context`.
    pub self_store_id: u64,

    pub poll_safe_point_interval: Duration,

    /// If this is set, safe_point will be checked before doing GC on every region while working.
    /// Otherwise safe_point will be only checked when `poll_safe_point_interval` has past since
    /// last checking.
    pub always_check_safe_point: bool,

    /// This will be called when a round of GC has finished and goes back to idle state.
    /// This field is for test purpose.
    pub post_a_round_of_gc: Option<Box<dyn Fn() + Send>>,

    pub ratio_threshold: f64,
}

impl<S: GcSafePointProvider, R: RegionInfoProvider> AutoGcConfig<S, R> {
    /// Creates a new config.
    pub fn new(safe_point_provider: S, region_info_provider: R, self_store_id: u64, ratio_threshold: f64) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_secs(POLL_SAFE_POINT_INTERVAL_SECS),
            always_check_safe_point: false,
            post_a_round_of_gc: None,
            ratio_threshold,
        }
    }

    /// Creates a config for test purpose. The interval to poll safe point is as short as 0.1s and
    /// during GC it never skips checking safe point.
    pub fn new_test_cfg(
        safe_point_provider: S,
        region_info_provider: R,
        self_store_id: u64,
    ) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_millis(100),
            always_check_safe_point: true,
            post_a_round_of_gc: None,
            ratio_threshold: 0.1
        }
    }
}

/// Used to check if `GcManager` should be stopped.
///
/// When `GcManager` is running, it might take very long time to GC a round. It should be able to
/// break at any time so that we can shut down TiKV in time.
pub(super) struct GcManagerContext {
    /// Whether an stop signal is received.
    progress: Option<Key>,
    processed_regions: usize,
    working: bool,
}

impl GcManagerContext {
    pub fn new() -> Self {
        Self {
            progress: None,
            processed_regions: 0,
            working: false,
        }
    }
}

/// Used to represent the state of `GcManager`.
#[derive(PartialEq)]
enum GcManagerState {
    None,
    Init,
    Idle,
    Working,
}

impl GcManagerState {
    pub fn tag(&self) -> &str {
        match self {
            GcManagerState::None => "",
            GcManagerState::Init => "initializing",
            GcManagerState::Idle => "idle",
            GcManagerState::Working => "working",
        }
    }
}

#[inline]
fn set_status_metrics(state: GcManagerState) {
    for s in &[
        GcManagerState::Init,
        GcManagerState::Idle,
        GcManagerState::Working,
    ] {
        AUTO_GC_STATUS_GAUGE_VEC
            .with_label_values(&[s.tag()])
            .set(if state == *s { 1 } else { 0 });
    }
}

#[derive(Display)]
pub enum GcMsg {
    Timeout,
    Tick,
}

/// Controls how GC runs automatically on the TiKV.
/// It polls safe point periodically, and when the safe point is updated, `GcManager` will start to
/// scan all regions (whose leader is on this TiKV), and does GC on all those regions.
pub(super) struct GcManager<S: GcSafePointProvider, R: RegionInfoProvider> {
    cfg: AutoGcConfig<S, R>,

    /// The current safe point. `GcManager` will try to update it periodically. When `safe_point` is
    /// updated, `GCManager` will start to do GC on all regions.
    safe_point: Arc<AtomicU64>,

    safe_point_last_check_time: Instant,

    /// Used to schedule `GcTask`s.
    worker_scheduler: FutureScheduler<GcTask>,
    engine: RocksEngine,
    scheduler: Option<Scheduler<GcMsg>>,

    /// Holds the running status. It will tell us if `GcManager` should stop working and exit.
    ctx: GcManagerContext,

    cfg_tracker: GcWorkerConfigManager,
    cluster_version: ClusterVersion,
}
impl<S: GcSafePointProvider, R: RegionInfoProvider> Runnable<GcMsg> for GcManager<S, R> {
    fn register_scheduler(&mut self, scheduler: Scheduler<GcMsg>) {
        scheduler.register_timeout(GcMsg::Timeout, self.cfg.poll_safe_point_interval);
        self.scheduler = Some(scheduler);
    }

    fn run(&mut self, msg: GcMsg) {
        match msg {
            GcMsg::Timeout => {
                AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                    .with_label_values(&[PROCESS_TYPE_GC])
                    .set(0);
                AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                    .with_label_values(&[PROCESS_TYPE_SCAN])
                    .set(0);

                set_status_metrics(GcManagerState::Idle);
                if !self.try_update_safe_point() {
                    return;
                }

                if self.ctx.working {
                    return;
                }

                // Don't need to run GC any more if compaction filter is enabled.
                if !is_compaction_filter_allowd(&*self.cfg_tracker.value(), &self.cluster_version) {
                    set_status_metrics(GcManagerState::Working);
                    self.ctx.working = true;
                    if self.start_gc_round() {
                        self.ctx.working = false;
                        if let Some(on_finished) = self.cfg.post_a_round_of_gc.as_ref() {
                            on_finished();
                        }
                    }
                }
                self.scheduler.as_ref().unwrap().register_timeout(GcMsg::Timeout, self.cfg.poll_safe_point_interval);
            },
            GcMsg::Tick => {
                if self.tick_gc() {
                    self.ctx.working = false;
                    if let Some(on_finished) = self.cfg.post_a_round_of_gc.as_ref() {
                        on_finished();
                    }
                }
            }
        }
    }
}

impl<S: GcSafePointProvider, R: RegionInfoProvider> Drop for GcManager<S, R> {
    fn drop(&mut self) {
        set_status_metrics(GcManagerState::None);
        debug!("gc-manager is stopped");
    }
}

impl<S: GcSafePointProvider, R: RegionInfoProvider> GcManager<S, R> {
    pub fn new(
        cfg: AutoGcConfig<S, R>,
        safe_point: Arc<AtomicU64>,
        engine: RocksEngine,
        worker_scheduler: FutureScheduler<GcTask>,
        cfg_tracker: GcWorkerConfigManager,
        cluster_version: ClusterVersion,
    ) -> GcManager<S, R> {
        GcManager {
            cfg,
            safe_point,
            safe_point_last_check_time: Instant::now(),
            worker_scheduler,
            engine,
            scheduler: None,
            ctx: GcManagerContext::new(),
            cfg_tracker,
            cluster_version,
        }
    }

    fn curr_safe_point(&self) -> TimeStamp {
        let ts = self.safe_point.load(AtomicOrdering::Relaxed);
        TimeStamp::new(ts)
    }

    fn save_safe_point(&self, ts: TimeStamp) {
        self.safe_point
            .store(ts.into_inner(), AtomicOrdering::Relaxed);
    }

    /// Sets the initial state of the `GCManger`.
    /// The only task of initializing is to simply get the current safe point as the initial value
    /// of `safe_point`. TiKV won't do any GC automatically until the first time `safe_point` was
    /// updated to a greater value than initial value.
    pub fn initialize(&mut self) {
        set_status_metrics(GcManagerState::Init);
        debug!("gc-manager is initializing");
        self.save_safe_point(TimeStamp::zero());
        self.try_update_safe_point();
        debug!("gc-manager started"; "safe_point" => self.curr_safe_point());
    }


    /// Tries to update the safe point. Returns true if safe point has been updated to a greater
    /// value. Returns false if safe point didn't change or we encountered an error.
    fn try_update_safe_point(&mut self) -> bool {
        self.safe_point_last_check_time = Instant::now();

        let safe_point = match self.cfg.safe_point_provider.get_safe_point() {
            Ok(res) => res,
            // Return false directly so we will check it a while later.
            Err(e) => {
                error!("failed to get safe point from pd"; "err" => ?e);
                return false;
            }
        };

        let old_safe_point = self.curr_safe_point();
        match safe_point.cmp(&old_safe_point) {
            Ordering::Less => {
                panic!(
                    "got new safe point {} which is less than current safe point {}. \
                     there must be something wrong.",
                    safe_point, old_safe_point,
                );
            }
            Ordering::Equal => false,
            Ordering::Greater => {
                debug!("gc_worker: update safe point"; "safe_point" => safe_point);
                self.save_safe_point(safe_point);
                AUTO_GC_SAFE_POINT_GAUGE.set(safe_point.into_inner() as i64);
                true
            }
        }
    }

    /// Scans all regions on the TiKV whose leader is this TiKV, and does GC on all of them.
    /// Regions are scanned and GC-ed in lexicographical order.
    ///
    /// While the `gc_a_round` function is running, it will periodically check whether safe_point is
    /// updated before the function `gc_a_round` finishes. If so, *Rewinding* will occur. For
    /// example, when we just starts to do GC, our progress is like this: ('^' means our current
    /// progress)
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ^
    /// ```
    ///
    /// And after a while, our GC progress is like this:
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------^
    /// ```
    ///
    /// At this time we found that safe point was updated, so rewinding will happen. First we
    /// continue working to the end: ('#' indicates the position that safe point updates)
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------#------------------------------------------^
    /// ```
    ///
    /// Then region 1-2 were GC-ed with the old safe point and region 3-6 were GC-ed with the new
    /// new one. Then, we *rewind* to the very beginning and continue GC to the position that safe
    /// point updates:
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------#------------------------------------------^
    /// ----------------------^
    /// ```
    ///
    /// Then GC finishes.
    /// If safe point updates again at some time, it will still try to GC all regions with the
    /// latest safe point. If safe point always updates before `gc_a_round` finishes, `gc_a_round`
    /// may never stop, but it doesn't matter.
    fn start_gc_round(&mut self) -> bool {
        // Represents where we should stop doing GC. `None` means the very end of the TiKV.
        self.ctx.progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));
        // Records how many region we have GC-ed.
        self.ctx.processed_regions = 0;
        info!(
            "gc_worker: start auto gc"; "safe_point" => self.curr_safe_point()
        );
        self.tick_gc()
    }

    fn tick_gc(&mut self) -> bool {
        // The following loop iterates all regions whose leader is on this TiKV and does GC on them.
        // At the same time, check whether safe_point is updated periodically. If it's updated,
        // rewinding will happen.
        // We are not going to rewind, So we will stop if `progress` reaches `end`.
        if let Some(progress) = self.ctx.progress.as_ref() {
            let from_key = progress.clone();
            drop(progress);
            self.gc_next_region(from_key);
            return false;
        }
        // We have worked to the end of the TiKV or our progress has reached `end`, and we
        // don't need to rewind. In this case, the round of GC has finished.
        info!("gc_worker: finished auto gc"; "processed_regions" => self.ctx.processed_regions);
        true
    }

    /// Does GC on the next region after `from_key`. Returns the end key of the region it processed.
    /// If we have processed to the end of all regions, returns `None`.
    fn gc_next_region(
        &mut self,
        from_key: Key,
    ) {
        // Get the information of the next region to do GC.
        let (range, next_key) = self.get_next_gc_context(from_key);
        let (region_id, start_key, end_key) = match range {
            Some((r, s, e)) => (r, s, e),
            None => {
                self.ctx.progress = None;
                return;
            },
        };

        let hex_start = hex::encode_upper(&start_key);
        let hex_end = hex::encode_upper(&end_key);
        debug!("trying gc"; "start_key" => &hex_start, "end_key" => &hex_end);
        self.ctx.processed_regions += 1;
        AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
            .with_label_values(&[PROCESS_TYPE_GC])
            .inc();

        self.ctx.progress = next_key;

        let scheduler = self.scheduler.as_ref().unwrap().clone();
        let callback = Box::new(move |_| {
            scheduler.schedule(GcMsg::Tick).unwrap();
        });
        let safe_point = self.curr_safe_point();
        if !self.need_gc(&start_key, &end_key, safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return;
        }
        self.worker_scheduler.schedule(GcTask::Gc {
                region_id,
                start_key,
                end_key,
                safe_point,
                callback,
            }).unwrap();
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do further check after
    /// getting snapshot.
    fn need_gc(&self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> bool {
        let start = keys::data_key(start_key);
        let end = keys::data_end_key(end_key);
        let collection = match self.engine
            .get_range_properties_cf(CF_WRITE, &start, &end) {
            Ok(c) => c,
            Err(_) => return true,
        };
        check_need_gc(safe_point, self.cfg.ratio_threshold, &collection)
    }

    /// Gets the next region with end_key greater than given key.
    /// Returns a tuple with 2 fields:
    /// the first is the next region can be sent to GC worker;
    /// the second is the next key which can be passed into this method later.
    #[allow(clippy::type_complexity)]
    fn get_next_gc_context(&mut self, key: Key) -> (Option<(u64, Vec<u8>, Vec<u8>)>, Option<Key>) {
        let (tx, rx) = mpsc::channel();
        let store_id = self.cfg.self_store_id;

        let res = self.cfg.region_info_provider.seek_region(
            key.as_encoded(),
            Box::new(move |iter| {
                let mut scanned_regions = 0;
                for info in iter {
                    scanned_regions += 1;
                    if find_peer(&info.region, store_id).is_some() {
                        let _ = tx.send((Some(info.region.clone()), scanned_regions));
                        return;
                    }
                }
                let _ = tx.send((None, scanned_regions));
            }),
        );

        if let Err(e) = res {
            error!(
                "gc_worker: failed to get next region information"; "err" => ?e
            );
            return (None, None);
        };

        let seek_region_res = rx.recv().map(|(region, scanned_regions)| {
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .add(scanned_regions);
            region
        });

        match seek_region_res {
            Ok(Some(mut region)) => {
                let r = region.get_id();
                let (s, e) = (region.take_start_key(), region.take_end_key());
                let next_key = if e.is_empty() {
                    None
                } else {
                    Some(Key::from_encoded_slice(&e))
                };
                (Some((r, s, e)), next_key)
            }
            Ok(None) => (None, None),
            Err(e) => {
                error!("failed to get next region information"; "err" => ?e);
                (None, None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Callback;
    use kvproto::metapb;
    use raft::StateRole;
    use raftstore::coprocessor::Result as CopResult;
    use raftstore::coprocessor::{RegionInfo, SeekRegionCallback};
    use raftstore::store::util::new_peer;
    use std::collections::BTreeMap;
    use std::mem;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use tikv_util::worker::{FutureRunnable, FutureWorker};
    use tokio_core::reactor::Handle;

    fn take_callback(t: &mut GcTask) -> Callback<()> {
        let callback = match t {
            GcTask::Gc {
                ref mut callback, ..
            } => callback,
            GcTask::UnsafeDestroyRange {
                ref mut callback, ..
            } => callback,
            GcTask::PhysicalScanLock { .. } => unreachable!(),
            GcTask::Validate(_) => unreachable!(),
        };
        mem::replace(callback, Box::new(|_| {}))
    }

    struct MockSafePointProvider {
        rx: Receiver<TimeStamp>,
    }

    impl GcSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> Result<TimeStamp> {
            // Error will be ignored by `GcManager`, which is equivalent to that the safe_point
            // is not updated.
            self.rx.try_recv().map_err(|e| box_err!(e))
        }
    }

    #[derive(Clone)]
    struct MockRegionInfoProvider {
        // start_key -> (region_id, end_key)
        regions: BTreeMap<Vec<u8>, RegionInfo>,
    }

    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> CopResult<()> {
            let from = from.to_vec();
            callback(&mut self.regions.range(from..).map(|(_, v)| v));
            Ok(())
        }
    }

    struct MockGcRunner {
        tx: Sender<GcTask>,
    }

    impl FutureRunnable<GcTask> for MockGcRunner {
        fn run(&mut self, mut t: GcTask, _handle: &Handle) {
            let cb = take_callback(&mut t);
            self.tx.send(t).unwrap();
            cb(Ok(()));
        }
    }

    /// A set of utilities that helps testing `GcManager`.
    /// The safe_point polling interval is set to 100 ms.
    struct GcManagerTestUtil {
        gc_manager: Option<GcManager<MockSafePointProvider, MockRegionInfoProvider>>,
        worker: FutureWorker<GcTask>,
        safe_point_sender: Sender<TimeStamp>,
        gc_task_receiver: Receiver<GcTask>,
    }

    impl GcManagerTestUtil {
        pub fn new(regions: BTreeMap<Vec<u8>, RegionInfo>) -> Self {
            let mut worker = FutureWorker::new("test-gc-worker");
            let (gc_task_sender, gc_task_receiver) = channel();
            worker.start(MockGcRunner { tx: gc_task_sender }).unwrap();

            let (safe_point_sender, safe_point_receiver) = channel();

            let mut cfg = AutoGcConfig::new(
                MockSafePointProvider {
                    rx: safe_point_receiver,
                },
                MockRegionInfoProvider { regions },
                1,
            );
            cfg.poll_safe_point_interval = Duration::from_millis(100);
            cfg.always_check_safe_point = true;

            let gc_manager = GcManager::new(
                cfg,
                Arc::new(AtomicU64::new(0)),
                worker.scheduler(),
                GcWorkerConfigManager::default(),
                Default::default(),
            );
            Self {
                gc_manager: Some(gc_manager),
                worker,
                safe_point_sender,
                gc_task_receiver,
            }
        }

        /// Collect `GcTask`s that `GcManager` tried to execute.
        pub fn collect_scheduled_tasks(&self) -> Vec<GcTask> {
            self.gc_task_receiver.try_iter().collect()
        }

        pub fn add_next_safe_point(&self, safe_point: impl Into<TimeStamp>) {
            self.safe_point_sender.send(safe_point.into()).unwrap();
        }

        pub fn stop(&mut self) {
            self.worker.stop().unwrap().join().unwrap();
        }
    }

    /// Run a round of auto GC and check if it correctly GC regions as expected.
    ///
    /// Param `regions` is a `Vec` of tuples which is `(start_key, end_key, region_id)`
    ///
    /// The first value in param `safe_points` will be used to initialize the GcManager, and the remaining
    /// values will be checked before every time GC-ing a region. If the length of `safe_points` is
    /// less than executed GC tasks, the last value will be used for extra GC tasks.
    ///
    /// Param `expected_gc_tasks` is a `Vec` of tuples which is `(region_id, safe_point)`.
    fn test_auto_gc(
        regions: Vec<(Vec<u8>, Vec<u8>, u64)>,
        safe_points: Vec<impl Into<TimeStamp> + Copy>,
        expected_gc_tasks: Vec<(u64, impl Into<TimeStamp>)>,
    ) {
        let regions: BTreeMap<_, _> = regions
            .into_iter()
            .map(|(start_key, end_key, id)| {
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                let info = RegionInfo::new(r, StateRole::Leader);
                (start_key, info)
            })
            .collect();

        let mut test_util = GcManagerTestUtil::new(regions);

        for safe_point in &safe_points {
            test_util.add_next_safe_point(*safe_point);
        }
        test_util.gc_manager.as_mut().unwrap().initialize();

        test_util.gc_manager.as_mut().unwrap().gc_a_round().unwrap();
        test_util.stop();

        let gc_tasks: Vec<_> = test_util
            .collect_scheduled_tasks()
            .iter()
            .map(|task| match task {
                GcTask::Gc {
                    region_id,
                    safe_point,
                    ..
                } => (*region_id, *safe_point),
                _ => unreachable!(),
            })
            .collect();

        // Following code asserts gc_tasks == expected_gc_tasks.
        assert_eq!(gc_tasks.len(), expected_gc_tasks.len());
        let all_passed = gc_tasks.into_iter().zip(expected_gc_tasks.into_iter()).all(
            |((region, safe_point), (expect_region, expect_safe_point))| {
                region == expect_region && safe_point == expect_safe_point.into()
            },
        );
        assert!(all_passed);
    }

    #[test]
    fn test_update_safe_point() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(233);
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 233.into());

        let (tx, rx) = channel();
        ThreadBuilder::new()
            .spawn(move || {
                let safe_point = gc_manager.wait_for_next_safe_point().unwrap();
                tx.send(safe_point).unwrap();
            })
            .unwrap();
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(234);
        assert_eq!(rx.recv().unwrap(), 234.into());

        test_util.stop();
    }

    #[test]
    fn test_gc_manager_initialize() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(0);
        test_util.add_next_safe_point(5);
        gc_manager.initialize();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 5.into());
    }

    #[test]
    fn test_auto_gc_a_round_without_rewind() {
        // First region starts with empty and last region ends with empty.
        let regions = vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );

        // First region doesn't starts with empty and last region doesn't ends with empty.
        let regions = vec![
            (b"0".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"8".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );
    }

    #[test]
    fn test_auto_gc_rewinding() {
        for regions in vec![
            // First region starts with empty and last region ends with empty.
            vec![
                (b"".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"".to_vec(), 4),
            ],
            // First region doesn't starts with empty and last region doesn't ends with empty.
            vec![
                (b"0".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"8".to_vec(), 4),
            ],
        ] {
            test_auto_gc(
                regions.clone(),
                vec![233, 234],
                vec![(1, 234), (2, 234), (3, 234), (4, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 234],
                vec![(1, 233), (2, 234), (3, 234), (4, 234), (1, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 233, 234],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 233),
                    (4, 234),
                    (1, 234),
                    (2, 234),
                    (3, 234),
                ],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 234, 235],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 235),
                    (1, 235),
                    (2, 235),
                    (3, 235),
                ],
            );

            let mut safe_points = vec![233, 233, 233, 234, 234, 234, 235];
            // The logic of `gc_a_round` wastes a loop when the last region's end_key is not null, so it
            // will check safe point one more time before GC-ing the first region after rewinding.
            if !regions.last().unwrap().1.is_empty() {
                safe_points.insert(5, 234);
            }
            test_auto_gc(
                regions.clone(),
                safe_points,
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 234),
                    (1, 234),
                    (2, 235),
                    (3, 235),
                    (4, 235),
                    (1, 235),
                ],
            );
        }
    }
}
