// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_rocks::raw_util::CFOptions;
use engine_rocks::{RocksEngine, RocksEngineIterator, RocksWriteBatchVec, RocksTablePropertiesCollection};
use engine_traits::{CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, MiscExt};
use engine_traits::{
    KvEngine, Mutable, TablePropertiesExt,
    WriteBatch, WriteBatchExt,
};
use kvproto::kvrpcpb::Context;
use kvproto::metapb;
use tempfile::{Builder, TempDir};
use txn_types::{Key, Value};
use raftstore::store::{RegionIterator, RegionSnapshot};

use crate::storage::config::BlockCacheConfig;
use crate::storage::kv::write_modifies;
use tikv_util::escape;
use tikv_util::time::ThreadReadId;
use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::{
    Callback, CbContext, Cursor, Engine, Error, ErrorInner, Iterator as EngineIterator, Modify,
    Result, ScanMode, Snapshot, WriteData,
};

pub use engine_rocks::RocksSnapshot;

const TEMP_DIR: &str = "";

/// The RocksEngine is based on `RocksDB`.
///
/// This is intended for **testing use only**.
#[derive(Clone)]
pub struct LocalEngine {
    engine: RocksEngine,
}

impl LocalEngine {
    pub fn new(engine: RocksEngine) -> LocalEngine {
        LocalEngine { engine }
    }
}

impl Engine for LocalEngine {
    type Snap = RegionSnapshot<RocksSnapshot>;

    fn local_snapshot(&self, start_key: &[u8], end_key: &[u8]) -> Result<Self::Snap> {
        let snap = self.engine.snapshot();
        let mut region = metapb::Region::default();
        region.set_start_key(start_key.to_vec());
        region.set_end_key(end_key.to_vec());
        Ok(RegionSnapshot::from_snapshot(Arc::new(snap), Arc::new(region)))
    }

    fn async_snapshot(
        &self,
        _: &Context,
        _: Option<ThreadReadId>,
        cb: Callback<Self::Snap>,
    ) -> Result<()> {
        let snap = Arc::new(self.engine.snapshot());
        let region = Arc::new(metapb::Region::default());
        cb((CbContext::new(), Ok(RegionSnapshot::from_snapshot(snap, region))));
        Ok(())
    }

    fn async_write(&self, _: &Context, batch: WriteData, cb: Callback<()>) -> Result<()> {
        if batch.modifies.is_empty() {
            return Err(Error::from(ErrorInner::EmptyRequest));
        }
        let ret = if self.engine.support_write_batch_vec() {
            let wb = RocksWriteBatchVec::with_capacity(&self.engine, 4096);
            write_modifies(wb, &self.engine, batch.modifies)
        } else {
            let wb = self.engine.write_batch();
            write_modifies(wb, &self.engine, batch.modifies)
        };
        cb((CbContext::new(), ret));
        Ok(())
    }
    fn get_properties_cf(
        &self,
        cf: CfName,
        start: &[u8],
        end: &[u8],
    ) -> Result<RocksTablePropertiesCollection> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .get_range_properties_cf(cf, &start, &end)
            .map_err(|e| e.into())
    }

    fn delete_files_in_range_cf(
        &self,
        cf: CfName,
        start: &[u8],
        end: &[u8],
    ) -> Result<()> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .delete_files_in_range_cf(cf, &start, &end, false)
            .map_err(|e| e.into())
    }

    fn delete_all_in_range_cf(
        &self,
        cf: CfName,
        start: &[u8],
        end: &[u8]) -> Result<()> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .delete_all_in_range_cf(cf, &start, &end, false)
            .map_err(|e| e.into())
    }
}