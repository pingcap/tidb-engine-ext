// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// Disable warnings for unused engine_rocks's feature.
#![allow(dead_code)]
#![allow(unused_variables)]
use std::{
    fmt::{self, Debug, Formatter},
    fs,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use engine_rocks::{RocksDbVector, RocksEngineIterator, RocksSnapshot};
use engine_traits::{
    Checkpointable, Checkpointer, DbVector, Error, IterOptions, Iterable, KvEngine, Peekable,
    ReadOptions, Result, SyncMutable,
};
use rocksdb::{Writable, DB};
use tikv_util::box_err;

use crate::{r2e, util::get_cf_handle};

pub struct FsStatsExt {
    pub used: u64,
    pub capacity: u64,
    pub available: u64,
}

pub type RawPSWriteBatchPtr = *mut ::std::os::raw::c_void;
pub type RawPSWriteBatchWrapperTag = u32;

// This is just a copy from engine_store_ffi::RawCppPtr
#[repr(C)]
#[derive(Debug)]
pub struct RawPSWriteBatchWrapper {
    pub ptr: RawPSWriteBatchPtr,
    pub type_: RawPSWriteBatchWrapperTag,
}

unsafe impl Send for RawPSWriteBatchWrapper {}

pub trait FFIHubInner {
    fn get_store_stats(&self) -> FsStatsExt;

    fn create_write_batch(&self) -> RawPSWriteBatchWrapper;

    fn destroy_write_batch(&self, wb_wrapper: &RawPSWriteBatchWrapper);

    fn consume_write_batch(&self, wb: RawPSWriteBatchPtr);

    fn write_batch_size(&self, wb: RawPSWriteBatchPtr) -> usize;

    fn write_batch_is_empty(&self, wb: RawPSWriteBatchPtr) -> bool;

    fn write_batch_merge(&self, lwb: RawPSWriteBatchPtr, rwb: RawPSWriteBatchPtr);

    fn write_batch_clear(&self, wb: RawPSWriteBatchPtr);

    fn write_batch_put_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8], page: &[u8]);

    fn write_batch_del_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8]);

    fn read_page(&self, page_id: &[u8]) -> Option<Vec<u8>>;

    fn scan_page(
        &self,
        start_page_id: &[u8],
        end_page_id: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>,
    );
}

pub trait FFIHub: FFIHubInner + Send + Sync {}

#[derive(Clone)]
pub struct RocksEngine {
    // Must ensure rocks is the first field, for RocksEngine::from_ref.
    // We must own a engine_rocks::RocksEngine, since TiKV has not decouple from engine_rocks yet.
    pub rocks: engine_rocks::RocksEngine,
    pub engine_store_server_helper: isize,
    pub pool_capacity: usize,
    pub pending_applies_count: Arc<AtomicUsize>,
    pub ffi_hub: Option<Arc<dyn FFIHubInner + Send + Sync>>,
}

impl std::fmt::Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiFlashEngine")
            .field("rocks", &self.rocks)
            .field(
                "engine_store_server_helper",
                &self.engine_store_server_helper,
            )
            .finish()
    }
}

impl RocksEngine {
    pub(crate) fn new(db: DB) -> RocksEngine {
        RocksEngine::from_db(Arc::new(db))
    }

    pub fn init(
        &mut self,
        engine_store_server_helper: isize,
        snap_handle_pool_size: usize,
        ffi_hub: Option<Arc<dyn FFIHubInner + Send + Sync>>,
    ) {
        self.engine_store_server_helper = engine_store_server_helper;
        self.pool_capacity = snap_handle_pool_size;
        self.pending_applies_count.store(0, Ordering::SeqCst);
        self.ffi_hub = ffi_hub;
    }

    pub fn from_rocks(rocks: engine_rocks::RocksEngine) -> Self {
        RocksEngine {
            rocks,
            engine_store_server_helper: 0,
            pool_capacity: 0,
            pending_applies_count: Arc::new(AtomicUsize::new(0)),
            ffi_hub: None,
        }
    }

    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine {
            rocks: engine_rocks::RocksEngine::from_db(db),
            engine_store_server_helper: 0,
            pool_capacity: 0,
            pending_applies_count: Arc::new(AtomicUsize::new(0)),
            ffi_hub: None,
        }
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const RocksEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        self.rocks.as_inner()
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.rocks.get_sync_db()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty
        // directory but db has not been created, `DB::list_column_families`
        // fails and we can clean up the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.rocks.set_shared_block_cache(enable);
    }

    pub fn support_multi_batch_write(&self) -> bool {
        self.rocks.support_multi_batch_write()
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

    fn snapshot(&self) -> RocksSnapshot {
        self.rocks.snapshot()
    }

    fn sync(&self) -> Result<()> {
        self.rocks.sync()
    }

    fn flush_metrics(&self, instance: &str) {
        self.rocks.flush_metrics(instance);
    }

    fn reset_statistics(&self) {
        self.rocks.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        self.rocks.bad_downcast()
    }

    // The whole point is:
    // 1. When `handle_pending_applies` is called by `on_timeout`, we can handle at
    // least one. 2. When `handle_pending_applies` is called when we receive a
    // new task,    or when `handle_pending_applies` need to handle multiple
    // snapshots.    We need to compare to what's in queue.

    fn can_apply_snapshot(&self, is_timeout: bool, new_batch: bool, _region_id: u64) -> bool {
        // is called after calling observer's pre_handle_snapshot
        let in_queue = self.pending_applies_count.load(Ordering::SeqCst);
        // if queue is full, we should begin to handle
        let can = if is_timeout && new_batch {
            true
        } else {
            in_queue > self.pool_capacity
        };
        fail::fail_point!("on_can_apply_snapshot", |e| e
            .unwrap()
            .parse::<bool>()
            .unwrap());
        can
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    #[cfg(not(any(test, feature = "testexport")))]
    fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut f = f;
        self.ffi_hub
            .as_ref()
            .unwrap()
            .scan_page(start_key.into(), end_key.into(), &mut f);
        Ok(())
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.rocks.iterator_opt(cf, opts)
    }
}

pub struct PsDbVector(Vec<u8>);

impl PsDbVector {
    pub fn from_raw(raw: Vec<u8>) -> PsDbVector {
        PsDbVector(raw)
    }
}

impl DbVector for PsDbVector {}

impl Deref for PsDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for PsDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for PsDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl Peekable for RocksEngine {
    #[cfg(any(test, feature = "testexport"))]
    type DbVector = RocksDbVector;

    #[cfg(any(test, feature = "testexport"))]
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        self.rocks.get_value_opt(opts, key)
    }

    #[cfg(any(test, feature = "testexport"))]
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        self.rocks.get_value_cf_opt(opts, cf, key)
    }

    #[cfg(not(any(test, feature = "testexport")))]
    type DbVector = PsDbVector;

    #[cfg(not(any(test, feature = "testexport")))]
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<PsDbVector>> {
        let result = self.ffi_hub.as_ref().unwrap().read_page(key);
        return match result {
            None => Ok(None),
            Some(v) => Ok(Some(PsDbVector::from_raw(v))),
        };
    }

    #[cfg(not(any(test, feature = "testexport")))]
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<PsDbVector>> {
        self.get_value_opt(opts, key)
    }
}

impl RocksEngine {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        crate::do_write(cf, key)
    }
}

impl SyncMutable for RocksEngine {
    #[cfg(any(test, feature = "testexport"))]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.get_sync_db().put(key, value).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testexport"))]
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let db = self.rocks.get_sync_db();
            let handle = get_cf_handle(&db, cf)?;
            return self
                .rocks
                .get_sync_db()
                .put_cf(handle, key, value)
                .map_err(r2e);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testexport"))]
    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.get_sync_db().delete(key).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testexport"))]
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let db = self.rocks.get_sync_db();
            let handle = get_cf_handle(&db, cf)?;
            return self.rocks.get_sync_db().delete_cf(handle, key).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "testexport")))]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ffi_hub.as_ref().unwrap().create_write_batch();
            self.ffi_hub
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ffi_hub
                .as_ref()
                .unwrap()
                .consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "testexport")))]
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ffi_hub.as_ref().unwrap().create_write_batch();
            self.ffi_hub
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ffi_hub
                .as_ref()
                .unwrap()
                .consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "testexport")))]
    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ffi_hub.as_ref().unwrap().create_write_batch();
            self.ffi_hub
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ffi_hub
                .as_ref()
                .unwrap()
                .consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "testexport")))]
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ffi_hub.as_ref().unwrap().create_write_batch();
            self.ffi_hub
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ffi_hub
                .as_ref()
                .unwrap()
                .consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}

pub struct TiFlashCheckpointer {}

impl Checkpointable for RocksEngine {
    type Checkpointer = TiFlashCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        Err(Error::Other("TiFlash don't support Checkpointable".into()))
    }
}

impl Checkpointer for TiFlashCheckpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()> {
        Err(Error::Other(
            "TiFlash don't support Checkpointer::create_at".into(),
        ))
    }
}
