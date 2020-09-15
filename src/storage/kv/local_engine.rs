// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use engine_rocks::{RocksEngine, RocksWriteBatchVec};
use engine_traits::{KvEngine, WriteBatch, WriteBatchExt};
use kvproto::kvrpcpb::Context;
use kvproto::metapb;
use raftstore::store::RegionSnapshot;

use crate::storage::kv::write_modifies;
use tikv_util::time::ThreadReadId;

use super::{Callback, CbContext, Engine, Error, ErrorInner, Result, WriteData};

pub use engine_rocks::RocksSnapshot;

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
    pub fn get_db(&self) -> &RocksEngine {
        &self.engine
    }
}

impl Engine for LocalEngine {
    type Snap = RegionSnapshot<RocksSnapshot>;

    fn local_snapshot(&self, start_key: &[u8], end_key: &[u8]) -> Result<Self::Snap> {
        let snap = self.engine.snapshot();
        let mut region = metapb::Region::default();
        region.set_start_key(start_key.to_vec());
        region.set_end_key(end_key.to_vec());
        Ok(RegionSnapshot::from_snapshot(
            Arc::new(snap),
            Arc::new(region),
        ))
    }

    fn async_snapshot(
        &self,
        _: &Context,
        _: Option<ThreadReadId>,
        cb: Callback<Self::Snap>,
    ) -> Result<()> {
        let snap = Arc::new(self.engine.snapshot());
        let region = Arc::new(metapb::Region::default());
        cb((
            CbContext::new(),
            Ok(RegionSnapshot::from_snapshot(snap, region)),
        ));
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
}
