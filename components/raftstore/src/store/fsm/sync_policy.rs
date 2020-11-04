// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::{Arc, Condvar, Mutex};

use engine_traits::KvEngine;
use engine_traits::RaftEngine;

use std::thread::{Builder, JoinHandle};
use tikv_util::time::Instant as TiInstant;

use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::SyncEventMetrics;
use bitflags::_core::time::Duration;

#[derive(Clone, Copy, Eq, PartialEq)]
enum SyncMode {
    SyncIO,
    AsyncIO,
    DelaySyncIO,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SyncState {
    Wait,
    Run,
    Stop,
}

#[derive(Clone)]
pub struct Notifier {
    cond: Arc<Condvar>,
    state: Arc<Mutex<SyncState>>,
    run: Arc<AtomicBool>,
}

impl Notifier {
    pub fn new() -> Notifier {
        Notifier {
            cond: Arc::new(Condvar::new()),
            state: Arc::new(Mutex::new(SyncState::Run)),
            run: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Return whether this thread need to stop
    pub fn wait<F>(&self, mut f: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let mut state = self.state.lock().unwrap();
        self.run.store(false, Ordering::Release);
        if *state == SyncState::Stop {
            return false;
        }
        *state = SyncState::Wait;

        // The condiction `f` must be changed before calling `maybe_notify` so that this thread
        // would not sleep when there is still some messages to deal with.
        while *state == SyncState::Wait && !f() {
            state = self.cond.wait(state).unwrap();
        }
        if *state == SyncState::Wait {
            *state = SyncState::Run;
            self.run.store(true, Ordering::Release);
            return true;
        } else {
            return false;
        }
    }

    pub fn maybe_notify(&self) {
        // Use atomic flag to avoid acquire lock.
        if !self.run.load(Ordering::Acquire) {
            let state = self.state.lock().unwrap();
            if *state == SyncState::Wait {
                self.cond.notify_one();
            }
        }
    }

    pub fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        if *state == SyncState::Wait {
            *state = SyncState::Stop;
            self.cond.notify_one();
        } else {
            *state = SyncState::Stop;
        }
    }
}

/// This class is for control the raft-engine wal 'sync' policy.
/// When regions received data, the 'sync' will be holded until reach
/// the deadline. After that, when 'sync' is called by any thread later,
/// the notifications will be sent to these unsynced regions.
pub struct SyncPolicy<EK: KvEngine, ER: RaftEngine> {
    pub metrics: SyncEventMetrics,
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    sync_mode: SyncMode,
    delay_sync_us: i64,

    /// The global-variables are for cooperate with other poll-worker threads.
    global_plan_sync_ts: Arc<AtomicI64>,
    global_last_sync_ts: Arc<AtomicI64>,

    /// Mark the last-sync-time of this thread, for checking other threads did 'sync' or not.
    local_last_sync_ts: i64,

    notifier: Notifier,
}

impl<EK: KvEngine, ER: RaftEngine> SyncPolicy<EK, ER> {
    pub fn new(
        raft_engine: ER,
        router: RaftRouter<EK, ER>,
        delay_sync_enabled: bool,
        enable_sync_io: bool,
        delay_sync_us: i64,
    ) -> SyncPolicy<EK, ER> {
        let current_ts = TiInstant::now_coarse().to_microsec();
        let sync_mode = if enable_sync_io {
            SyncMode::AsyncIO
        } else if delay_sync_enabled {
            SyncMode::DelaySyncIO
        } else {
            SyncMode::SyncIO
        };
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            raft_engine,
            router,
            sync_mode,
            delay_sync_us,
            global_plan_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            global_last_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            local_last_sync_ts: current_ts,
            notifier: Notifier::new(),
        }
    }

    pub fn clone(&self) -> SyncPolicy<EK, ER> {
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            raft_engine: self.raft_engine.clone(),
            router: self.router.clone(),
            sync_mode: self.sync_mode,
            delay_sync_us: self.delay_sync_us,
            global_plan_sync_ts: self.global_plan_sync_ts.clone(),
            global_last_sync_ts: self.global_last_sync_ts.clone(),
            local_last_sync_ts: self.global_last_sync_ts.load(Ordering::Relaxed),
            notifier: self.notifier.clone(),
        }
    }

    pub fn current_ts(&self) -> i64 {
        TiInstant::now_coarse().to_microsec()
    }

    pub fn try_refresh_last_sync_ts(&mut self) -> i64 {
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        if self.local_last_sync_ts < last_sync_ts {
            self.local_last_sync_ts = last_sync_ts;
        }
        self.local_last_sync_ts
    }

    /// Return if all unsynced regions are flushed
    pub fn try_flush_regions(&mut self) {
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        if self.local_last_sync_ts < last_sync_ts {
            self.local_last_sync_ts = last_sync_ts;
        }
    }

    /// Check if this thread should call sync or not.
    pub fn maybe_sync(&mut self) -> bool {
        let before_sync_ts = TiInstant::now_coarse().to_microsec();
        self.try_flush_regions();
        match self.sync_mode {
            SyncMode::DelaySyncIO => {
                let sync = self.check_sync_internal(before_sync_ts);
                if sync {
                    self.raft_engine.sync().unwrap_or_else(|e| {
                        panic!("failed to sync raft engine: {:?}", e);
                    });
                    self.metrics.sync_events.sync_raftdb_count += 1;
                    self.post_sync_time_point(before_sync_ts);
                } else {
                    self.metrics.sync_events.raftdb_skipped_sync_count += 1;
                }
                sync
            }
            SyncMode::SyncIO => {
                self.raft_engine.sync().unwrap_or_else(|e| {
                    panic!("failed to sync raft engine: {:?}", e);
                });
                true
            }
            SyncMode::AsyncIO => {
                let mut plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
                while plan_sync_ts < before_sync_ts {
                    // We will refresh the plan sync ts to notify sync-thread. If `global_plan_sync_ts`
                    //  has been refreshed by other thread we must make sure that it is greater
                    // than time of current thread, or it may make sync-thread change `global_last_sync_ts`
                    // to a smaller value and ready of current thread will never be flushed if there is no
                    // write request any more.
                    let old_ts = self.global_plan_sync_ts.compare_and_swap(
                        plan_sync_ts,
                        before_sync_ts,
                        Ordering::SeqCst,
                    );
                    if plan_sync_ts == old_ts {
                        self.notifier.maybe_notify();
                        break;
                    } else {
                        plan_sync_ts = old_ts;
                    }
                }
                false
            }
        }
    }

    /// Update metrics and status after the sync time point (whether did sync or not)
    fn post_sync_time_point(&mut self, before_sync_ts: i64) {
        self.update_status_after_synced(before_sync_ts);
    }

    /// Try to sync and flush unsynced regions, it's called when no 'ready' comes.
    /// Return if all unsynced regions are flushed.
    pub fn try_sync_and_flush(&mut self) {
        match self.sync_mode {
            SyncMode::SyncIO => return,
            SyncMode::AsyncIO => {
                self.try_flush_regions();
                return;
            }
            SyncMode::DelaySyncIO => {
                self.try_flush_regions();
            }
        }

        let before_sync_ts = self.current_ts();
        if !self.check_sync_internal(before_sync_ts) {
            return;
        }

        if let Err(e) = self.raft_engine.sync() {
            panic!("failed to sync raft engine: {:?}", e);
        }

        self.metrics.sync_events.sync_raftdb_count += 1;
        self.metrics.sync_events.sync_raftdb_reach_deadline_no_ready += 1;

        self.update_status_after_synced(before_sync_ts);
    }

    /// Update the global status(last_sync_ts, last_plan_ts),
    fn update_status_after_synced(&mut self, before_sync_ts: i64) {
        self.local_last_sync_ts = before_sync_ts;
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        let pre_ts = self.global_last_sync_ts.compare_and_swap(
            last_sync_ts,
            before_sync_ts,
            Ordering::AcqRel,
        );
        assert_eq!(
            pre_ts, last_sync_ts,
            "failed to CAS last sync ts, pre ts {} != last sync ts {}",
            pre_ts, last_sync_ts
        );
    }

    /// Check if this thread should call sync or not.
    /// If true, the global_plan_sync_ts will be updated to before_sync_ts.
    /// If current_ts is close to last_sync_ts, or other threads are planning
    /// to sync, then this thread should not sync.
    fn check_sync_internal(&mut self, before_sync_ts: i64) -> bool {
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
        if last_sync_ts != plan_sync_ts {
            // Another thread is planning to sync, so this thread should do nothing
            assert!(
                plan_sync_ts > last_sync_ts,
                "plan sync ts {} < last sync ts {}",
                plan_sync_ts,
                last_sync_ts
            );
            return false;
        }
        if before_sync_ts <= last_sync_ts {
            return false;
        }

        let need_sync = {
            let elapsed = before_sync_ts - last_sync_ts;
            self.metrics
                .thread_check_result
                .observe(elapsed as f64 / 1e9);

            if elapsed >= self.delay_sync_us {
                self.metrics.sync_events.sync_raftdb_reach_deadline += 1;
                true
            } else {
                false
            }
        };

        if !need_sync {
            return false;
        }

        // If it's false, it means another thread is planning to sync, so this thread should do nothing
        plan_sync_ts
            == self.global_plan_sync_ts.compare_and_swap(
                plan_sync_ts,
                before_sync_ts,
                Ordering::AcqRel,
            )
    }

    fn run(&mut self) {
        let mut plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
        const SLEEP_DELAY_US: u64 = 200;
        loop {
            if plan_sync_ts <= self.local_last_sync_ts {
                if !self.wait() {
                    // Sync again and exit.
                    let before_sync_ts = self.current_ts();
                    self.raft_engine.sync().unwrap_or_else(|e| {
                        panic!("failed to sync raft engine: {:?}", e);
                    });
                    self.update_status_after_synced(before_sync_ts);
                    return;
                }
            }
            let mut before_sync_ts = self.current_ts();
            while before_sync_ts - self.local_last_sync_ts < self.delay_sync_us {
                std::thread::sleep(Duration::from_micros(SLEEP_DELAY_US));
                before_sync_ts = self.current_ts();
            }
            self.raft_engine.sync().unwrap_or_else(|e| {
                panic!("failed to sync raft engine: {:?}", e);
            });
            self.metrics.sync_events.sync_raftdb_count += 1;
            self.update_status_after_synced(before_sync_ts);
            plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
        }
    }

    fn wait(&mut self) -> bool {
        let start_wait_time = self.current_ts();
        const TIME_PER_SLEEP: i64 = 50;
        while self.current_ts() - start_wait_time < TIME_PER_SLEEP {
            if self.global_plan_sync_ts.load(Ordering::Acquire) > self.local_last_sync_ts {
                return true;
            }
            std::thread::yield_now();
        }
        let global_plan_sync_ts = self.global_plan_sync_ts.clone();
        let local_sync_ts = self.local_last_sync_ts;
        self.notifier
            .wait(move || global_plan_sync_ts.load(Ordering::Acquire) > local_sync_ts)
    }
}

pub struct SyncController {
    handle: JoinHandle<()>,
    notifier: Notifier,
}

impl SyncController {
    pub fn stop(self) {
        self.notifier.stop();
        self.handle.join().unwrap();
    }
}

pub fn run_sync_thread<EK: KvEngine, ER: RaftEngine>(
    mut policy: SyncPolicy<EK, ER>,
) -> SyncController {
    let notifier = policy.notifier.clone();
    let handle = Builder::new()
        .name(thd_name!("sync-raft-wal"))
        .spawn(move || {
            policy.run();
        })
        .unwrap();
    SyncController { notifier, handle }
}
