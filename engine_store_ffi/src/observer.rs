// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::hash_map::Entry as MapEntry,
    io::Write,
    ops::DerefMut,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex, RwLock,
    },
    time::SystemTime,
};

use collections::HashMap;
use engine_tiflash::{FsStatsExt, RawPSWriteBatchPtr, RawPSWriteBatchWrapper};
use engine_traits::{RaftEngine, SstMetaInfo, CF_RAFT};
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, RaftCmdRequest},
    raft_serverpb::{PeerState, RaftApplyState, RaftMessage, RegionLocalState},
};
use protobuf::Message;
use raft::{eraftpb, eraftpb::MessageType, StateRole};
use raftstore::{
    coprocessor::{
        AdminObserver, ApplyCtxInfo, ApplySnapshotObserver, BoxAdminObserver,
        BoxApplySnapshotObserver, BoxPdTaskObserver, BoxQueryObserver, BoxRegionChangeObserver,
        BoxUpdateSafeTsObserver, Cmd, Coprocessor, CoprocessorHost, ObserverContext,
        PdTaskObserver, QueryObserver, RegionChangeEvent, RegionChangeObserver, RegionState,
        StoreSizeInfo, UpdateSafeTsObserver,
    },
    store::{
        self, check_sst_for_ingestion,
        snap::{plain_file_used, SnapEntry},
        SnapKey, SnapManager, Transport,
    },
    Error as RaftStoreError, Result as RaftStoreResult,
};
use sst_importer::SstImporter;
use tikv_util::{box_err, crit, debug, defer, error, info, store::find_peer, warn};
use yatp::{
    pool::{Builder, ThreadPool},
    task::future::TaskCell,
};

use crate::{
    gen_engine_store_server_helper,
    interfaces::root::{DB as ffi_interfaces, DB::EngineStoreApplyRes},
    name_to_cf, ColumnFamilyType, EngineStoreServerHelper, RaftCmdHeader, RawCppPtr, TiFlashEngine,
    WriteCmdType, WriteCmds, CF_LOCK,
};

macro_rules! fatal {
    ($lvl:expr $(, $arg:expr)*) => ({
        crit!($lvl $(, $arg)*);
        ::std::process::exit(1)
    })
}

#[allow(clippy::from_over_into)]
impl Into<engine_tiflash::FsStatsExt> for ffi_interfaces::StoreStats {
    fn into(self) -> FsStatsExt {
        FsStatsExt {
            available: self.fs_stats.avail_size,
            capacity: self.fs_stats.capacity_size,
            used: self.fs_stats.used_size,
        }
    }
}

impl From<RawCppPtr> for RawPSWriteBatchWrapper {
    fn from(src: RawCppPtr) -> Self {
        let result = RawPSWriteBatchWrapper {
            ptr: src.ptr,
            type_: src.type_,
        };
        let mut src = src;
        src.ptr = std::ptr::null_mut();
        result
    }
}

pub struct TiFlashFFIHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
}
unsafe impl Send for TiFlashFFIHub {}
unsafe impl Sync for TiFlashFFIHub {}
impl engine_tiflash::FFIHubInner for TiFlashFFIHub {
    fn get_store_stats(&self) -> engine_tiflash::FsStatsExt {
        self.engine_store_server_helper
            .handle_compute_store_stats()
            .into()
    }

    fn create_write_batch(&self) -> RawPSWriteBatchWrapper {
        self.engine_store_server_helper.create_write_batch().into()
    }

    fn destroy_write_batch(&self, wb_wrapper: &RawPSWriteBatchWrapper) {
        self.engine_store_server_helper
            .gc_raw_cpp_ptr(wb_wrapper.ptr, wb_wrapper.type_);
    }

    fn consume_write_batch(&self, wb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.consume_write_batch(wb)
    }

    fn write_batch_size(&self, wb: RawPSWriteBatchPtr) -> usize {
        self.engine_store_server_helper.write_batch_size(wb) as usize
    }

    fn write_batch_is_empty(&self, wb: RawPSWriteBatchPtr) -> bool {
        self.engine_store_server_helper.write_batch_is_empty(wb) != 0
    }

    fn write_batch_merge(&self, lwb: RawPSWriteBatchPtr, rwb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.write_batch_merge(lwb, rwb)
    }

    fn write_batch_clear(&self, wb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.write_batch_clear(wb)
    }

    fn write_batch_put_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8], page: &[u8]) {
        self.engine_store_server_helper
            .write_batch_put_page(wb, page_id.into(), page.into())
    }

    fn write_batch_del_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8]) {
        self.engine_store_server_helper
            .write_batch_del_page(wb, page_id.into())
    }

    fn read_page(&self, page_id: &[u8]) -> Option<Vec<u8>> {
        let value = self.engine_store_server_helper.read_page(page_id.into());
        return if value.view.len == 0 {
            None
        } else {
            Some(value.view.to_slice().to_vec())
        };
    }

    fn scan_page(
        &self,
        start_page_id: &[u8],
        end_page_id: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> engine_traits::Result<bool>,
    ) {
        let values = self
            .engine_store_server_helper
            .scan_page(start_page_id.into(), end_page_id.into());
        for i in 0..values.len {
            let value = unsafe { &*values.inner.offset(i as isize) };
            if value.page_view.len != 0 {
                f(
                    &value.key_view.to_slice().to_vec(),
                    &value.page_view.to_slice().to_vec(),
                );
            }
        }
    }
}

pub struct PtrWrapper(RawCppPtr);

unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

#[derive(Default, Debug)]
pub struct PrehandleContext {
    // tracer holds ptr of snapshot prehandled by TiFlash side.
    pub tracer: HashMap<SnapKey, Arc<PrehandleTask>>,
}

#[derive(Debug)]
pub struct PrehandleTask {
    pub recv: mpsc::Receiver<PtrWrapper>,
    pub peer_id: u64,
}

impl PrehandleTask {
    fn new(recv: mpsc::Receiver<PtrWrapper>, peer_id: u64) -> Self {
        PrehandleTask { recv, peer_id }
    }
}
unsafe impl Send for PrehandleTask {}
unsafe impl Sync for PrehandleTask {}

const CACHED_REGION_INFO_SLOT_COUNT: usize = 256;

#[derive(Debug, Default)]
pub struct CachedRegionInfo {
    pub replicated_or_created: AtomicBool,
    // TiKV assumes a region's learner peer is added through snapshot.
    // If this field is false, will try fast path when meet MsgAppend.
    // If this field is true, it means this peer is inited or will be inited by a TiKV snapshot.
    // NOTE If we want a fallback, then we must set inited_or_fallback to true,
    // Otherwise, a normal snapshot will be neglect in `post_apply_snapshot` and cause data loss.
    pub inited_or_fallback: AtomicBool,
    pub snapshot_inflight: portable_atomic::AtomicU128,
}

pub type CachedRegionInfoMap = HashMap<u64, Arc<CachedRegionInfo>>;

pub struct TiFlashObserver<T: Transport, ER: RaftEngine> {
    pub store_id: u64,
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
    pub engine: TiFlashEngine,
    pub raft_engine: ER,
    pub sst_importer: Arc<SstImporter>,
    pub pre_handle_snapshot_ctx: Arc<Mutex<PrehandleContext>>,
    pub snap_handle_pool_size: usize,
    pub apply_snap_pool: Option<Arc<ThreadPool<TaskCell>>>,
    pub pending_delete_ssts: Arc<RwLock<Vec<SstMetaInfo>>>,
    pub cached_region_info: Arc<Vec<RwLock<CachedRegionInfoMap>>>,
    // TODO should we use a Mutex here?
    pub trans: Arc<Mutex<T>>,
    pub snap_mgr: Arc<SnapManager>,
    pub engine_store_cfg: crate::EngineStoreConfig,
}

impl<T: Transport + 'static, ER: RaftEngine> Clone for TiFlashObserver<T, ER> {
    fn clone(&self) -> Self {
        TiFlashObserver {
            store_id: self.store_id,
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            raft_engine: self.raft_engine.clone(),
            sst_importer: self.sst_importer.clone(),
            pre_handle_snapshot_ctx: self.pre_handle_snapshot_ctx.clone(),
            snap_handle_pool_size: self.snap_handle_pool_size,
            apply_snap_pool: self.apply_snap_pool.clone(),
            pending_delete_ssts: self.pending_delete_ssts.clone(),
            cached_region_info: self.cached_region_info.clone(),
            trans: self.trans.clone(),
            snap_mgr: self.snap_mgr.clone(),
            engine_store_cfg: self.engine_store_cfg.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to
// avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

// Credit: [splitmix64 algorithm](https://xorshift.di.unimi.it/splitmix64.c)
#[inline]
fn hash_u64(mut i: u64) -> u64 {
    i = (i ^ (i >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    i = (i ^ (i >> 27)).wrapping_mul(0x94d049bb133111eb);
    i ^ (i >> 31)
}

#[allow(dead_code)]
#[inline]
fn unhash_u64(mut i: u64) -> u64 {
    i = (i ^ (i >> 31) ^ (i >> 62)).wrapping_mul(0x319642b2d24d8ec3);
    i = (i ^ (i >> 27) ^ (i >> 54)).wrapping_mul(0x96de1b173f119089);
    i ^ (i >> 30) ^ (i >> 60)
}

pub fn validate_remote_peer_region(
    new_region: &kvproto::metapb::Region,
    store_id: u64,
    new_peer_id: u64,
) -> bool {
    match find_peer(new_region, store_id) {
        Some(peer) => peer.get_id() == new_peer_id,
        None => false,
    }
}

pub fn get_region_local_state<EK: engine_traits::KvEngine>(
    engine: &EK,
    region_id: u64,
) -> Option<RegionLocalState> {
    let region_state_key = keys::region_state_key(region_id);
    engine
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        .unwrap_or(None)
}

impl<T: Transport + 'static, ER: RaftEngine> TiFlashObserver<T, ER> {
    #[inline]
    fn slot_index(id: u64) -> usize {
        debug_assert!(CACHED_REGION_INFO_SLOT_COUNT.is_power_of_two());
        hash_u64(id) as usize & (CACHED_REGION_INFO_SLOT_COUNT - 1)
    }

    pub fn access_cached_region_info_mut<F: FnMut(MapEntry<u64, Arc<CachedRegionInfo>>)>(
        &self,
        region_id: u64,
        mut f: F,
    ) -> RaftStoreResult<()> {
        let slot_id = Self::slot_index(region_id);
        let mut guard = match self.cached_region_info.get(slot_id).unwrap().write() {
            Ok(g) => g,
            Err(_) => return Err(box_err!("access_cached_region_info_mut poisoned")),
        };
        f(guard.entry(region_id));
        Ok(())
    }

    pub fn remove_cached_region_info(&self, region_id: u64) {
        let slot_id = Self::slot_index(region_id);
        if let Ok(mut g) = self.cached_region_info.get(slot_id).unwrap().write() {
            info!(
                "remove_cached_region_info";
                "region_id" => region_id,
                "store_id" => self.store_id,
            );
            let _ = g.remove(&region_id);
        }
    }

    pub fn set_inited_or_fallback(&self, region_id: u64, v: bool) -> RaftStoreResult<()> {
        self.access_cached_region_info_mut(
            region_id,
            |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                MapEntry::Occupied(mut o) => {
                    o.get_mut().inited_or_fallback.store(v, Ordering::SeqCst);
                }
                MapEntry::Vacant(_) => {
                    tikv_util::safe_panic!("not inited!");
                }
            },
        )
    }

    pub fn set_snapshot_inflight(&self, region_id: u64, v: u128) -> RaftStoreResult<()> {
        self.access_cached_region_info_mut(
            region_id,
            |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                MapEntry::Occupied(mut o) => {
                    o.get_mut().snapshot_inflight.store(v, Ordering::SeqCst);
                }
                MapEntry::Vacant(_) => {
                    tikv_util::safe_panic!("not inited!");
                }
            },
        )
    }

    fn fallback_to_slow_path(&self, region_id: u64) {
        // TODO clean local, and prepare to request snapshot from TiKV as a trivial
        // procedure.
        fail::fail_point!("fallback_to_slow_path_not_allow", |_| {});
        if self.set_inited_or_fallback(region_id, true).is_err() {
            tikv_util::safe_panic!("set_inited_or_fallback");
        }
    }

    pub fn is_initialized(&self, region_id: u64) -> bool {
        match get_region_local_state(&self.engine, region_id) {
            None => false,
            Some(r) => {
                raftstore::store::util::is_region_initialized(r.get_region())
                    && (r.get_state() != PeerState::Tombstone)
            }
        }
    }

    // Returns whether we need to ignore this message and run fast path instead.
    pub fn maybe_fast_path(&self, msg: &RaftMessage) -> bool {
        if !self.engine_store_cfg.enable_fast_add_peer {
            // fast path not enabled
            return false;
        }
        // TODO Need to recover all region infomation from restart.
        let inner_msg = msg.get_message();
        if inner_msg.get_msg_type() != MessageType::MsgAppend {
            // we only handles the first MsgAppend
            return false;
        }
        let region_id = msg.get_region_id();
        let new_peer_id = msg.get_to_peer().get_id();
        let mut is_first = false;
        let mut is_replicated = false;
        let mut has_already_inited = None;
        let mut early_skip = false;
        let f = |info: MapEntry<u64, Arc<CachedRegionInfo>>| {
            match info {
                MapEntry::Occupied(mut o) => {
                    (is_first, has_already_inited) =
                        if !o.get().inited_or_fallback.load(Ordering::SeqCst) {
                            // If `has_already_inited` is true, usually means we recover from a
                            // restart. So we have data in disk, but not
                            // in memory. TODO maybe only check once, or
                            // we can remove apply snapshot.
                            let has_already_inited = self.is_initialized(region_id);
                            if has_already_inited {
                                o.get_mut().inited_or_fallback.store(true, Ordering::SeqCst);
                            }
                            (!has_already_inited, Some(has_already_inited))
                        } else {
                            (false, None)
                        };
                    // TODO include create
                    is_replicated = o.get().replicated_or_created.load(Ordering::SeqCst);
                    if is_first {
                        #[cfg(any(test, feature = "testexport"))]
                        {
                            info!("fast path: ongoing {}:{} {}, MsgAppend skipped",
                                self.store_id, region_id, new_peer_id;
                                    "to_peer_id" => msg.get_to_peer().get_id(),
                                    "from_peer_id" => msg.get_from_peer().get_id(),
                                    "inner_msg" => ?inner_msg,
                                    "is_replicated" => is_replicated,
                                    "has_already_inited" => has_already_inited,
                                    "is_first" => is_first,
                            );
                        }
                    }
                    let last = o.get().snapshot_inflight.load(Ordering::SeqCst);
                    if last != 0 {
                        let current = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap();
                        info!("fast path: ongoing {}:{} {}, MsgAppend duplicated",
                            self.store_id, region_id, new_peer_id;
                                "to_peer_id" => msg.get_to_peer().get_id(),
                                "from_peer_id" => msg.get_from_peer().get_id(),
                                "inner_msg" => ?inner_msg,
                                "is_replicated" => is_replicated,
                                "has_already_inited" => has_already_inited,
                                "is_first" => is_first,
                                "elapsed" => current.as_millis() - last,
                        );
                        early_skip = true;
                    }
                }
                MapEntry::Vacant(v) => {
                    info!("fast path: ongoing {}:{} {}, first message", self.store_id, region_id, new_peer_id;
                        "to_peer_id" => msg.get_to_peer().get_id(),
                        "from_peer_id" => msg.get_from_peer().get_id(),
                        "inner_msg" => ?inner_msg,
                    );
                    v.insert(Arc::new(CachedRegionInfo::default()));
                    is_first = true;
                }
            }
        };
        // Can use immutable version.
        self.access_cached_region_info_mut(region_id, f).unwrap();

        if !is_first {
            #[cfg(any(test, feature = "testexport"))]
            {
                info!(
                    "fast path: normal MsgAppend of {}:{} {}",
                    self.store_id, region_id, new_peer_id;
                    "to_peer_id" => msg.get_to_peer().get_id(),
                    "from_peer_id" => msg.get_from_peer().get_id(),
                    "inner_msg" => ?inner_msg,
                );
            }
            return false;
        }

        if early_skip {
            return true;
        }

        {
            // Peer is not created by Peer::replicate, will cause RegionNotRegistered error,
            // see `check_msg`.
            if !is_replicated {
                info!("fast path: ongoing {}:{} {}, wait replicating peer", self.store_id, region_id, new_peer_id;
                    "to_peer_id" => msg.get_to_peer().get_id(),
                    "from_peer_id" => msg.get_from_peer().get_id(),
                    "inner_msg" => ?inner_msg,
                );
                return true;
            }
        }

        info!("fast path: ongoing {}:{} {}, fetch data from remote peer", self.store_id, region_id, new_peer_id;
            "to_peer_id" => msg.get_to_peer().get_id(),
            "from_peer_id" => msg.get_from_peer().get_id(),
        );
        fail::fail_point!("go_fast_path_not_allow", |_| { return false });
        fail::fail_point!("ffi_fast_add_peer_pause", |_| { return false });
        // Feed data
        let res = self
            .engine_store_server_helper
            .fast_add_peer(region_id, new_peer_id);
        match res.status {
            crate::FastAddPeerStatus::Ok => (),
            crate::FastAddPeerStatus::WaitForData => {
                info!(
                    "fast path: ongoing {}:{} {}. remote peer preparing data, wait",
                    self.store_id, region_id, new_peer_id
                );
                return true;
            }
            _ => {
                error!(
                    "fast path: ongoing {}:{} {} failed. fetch and replace error {:?}, fallback to normal",
                    self.store_id, region_id, new_peer_id, res
                );
                self.fallback_to_slow_path(region_id);
                return false;
            }
        };

        let apply_state_str = res.apply_state.view.to_slice();
        let region_str = res.region.view.to_slice();
        let mut apply_state = RaftApplyState::default();
        let mut new_region = kvproto::metapb::Region::default();
        apply_state.merge_from_bytes(apply_state_str).unwrap();
        new_region.merge_from_bytes(region_str).unwrap();

        // Validate
        // check if the source already knows the know peer
        if !validate_remote_peer_region(&new_region, self.store_id, new_peer_id) {
            info!(
                "fast path: ongoing {}:{} {}. failed remote peer has not applied conf change",
                self.store_id, region_id, new_peer_id;
                "region" => ?new_region,
            );
            self.fallback_to_slow_path(region_id);
            return false;
        }

        info!("fast path: ongoing {}:{} {}, start build and send", self.store_id, region_id, new_peer_id;
            "to_peer_id" => msg.get_to_peer().get_id(),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "new_region" => ?new_region,
            "apply_state" => ?apply_state,
        );
        match self.build_and_send_snapshot(region_id, new_peer_id, msg, apply_state, new_region) {
            Ok(s) => {
                match s {
                    crate::FastAddPeerStatus::Ok => {
                        fail::fail_point!("go_fast_path_succeed", |_| { return false });
                        info!("fast path: ongoing {}:{} {}, finish build and send", self.store_id, region_id, new_peer_id;
                            "to_peer_id" => msg.get_to_peer().get_id(),
                            "from_peer_id" => msg.get_from_peer().get_id(),
                        );
                    }
                    crate::FastAddPeerStatus::WaitForData => {
                        info!(
                            "fast path: ongoing {}:{} {}. remote peer preparing data, wait",
                            new_peer_id, self.store_id, region_id
                        );
                        return true;
                    }
                    _ => {
                        error!(
                            "fast path: ongoing {}:{} {} failed. build and sent snapshot code {:?}",
                            self.store_id, region_id, new_peer_id, s
                        );
                        self.fallback_to_slow_path(region_id);
                        return false;
                    }
                };
            }
            Err(e) => {
                error!(
                    "fast path: ongoing {}:{} {} failed. build and sent snapshot error {:?}",
                    self.store_id, region_id, new_peer_id, e
                );
                self.fallback_to_slow_path(region_id);
                return false;
            }
        };
        is_first
    }

    fn check_entry_at_index(
        &self,
        region_id: u64,
        index: u64,
        peer_id: u64,
        tag: &str,
    ) -> RaftStoreResult<u64> {
        match self.raft_engine.get_entry(region_id, index)? {
            Some(entry) => Ok(entry.get_term()),
            None => {
                return Err(box_err!(
                    "can't find entry for index {} of region {}, peer_id: {}, tag {}",
                    index,
                    region_id,
                    peer_id,
                    tag
                ));
            }
        }
    }

    fn build_and_send_snapshot(
        &self,
        region_id: u64,
        new_peer_id: u64,
        msg: &RaftMessage,
        apply_state: RaftApplyState,
        new_region: kvproto::metapb::Region,
    ) -> RaftStoreResult<crate::FastAddPeerStatus> {
        let inner_msg = msg.get_message();
        // Build snapshot by get_snapshot_for_building
        let (snap, key) = {
            // Find term of entry at applied_index.
            let applied_index = apply_state.get_applied_index();
            let applied_term =
                self.check_entry_at_index(region_id, applied_index, new_peer_id, "applied_index")?;
            // Will otherwise cause "got message with lower index than committed" loop.
            self.check_entry_at_index(
                region_id,
                apply_state.get_commit_index(),
                new_peer_id,
                "commit_index",
            )?;

            let key = SnapKey::new(region_id, applied_term, applied_index);
            self.snap_mgr.register(key.clone(), SnapEntry::Generating);
            defer!(self.snap_mgr.deregister(&key, &SnapEntry::Generating));
            let snapshot = self.snap_mgr.get_snapshot_for_building(&key)?;

            (snapshot, key.clone())
        };

        // Build snapshot by do_snapshot
        let mut pb_snapshot: eraftpb::Snapshot = Default::default();
        let pb_snapshot_metadata: &mut eraftpb::SnapshotMetadata = pb_snapshot.mut_metadata();
        let mut snap_data = kvproto::raft_serverpb::RaftSnapshotData::default();
        {
            // eraftpb::SnapshotMetadata
            for (_, cf) in raftstore::store::snap::SNAPSHOT_CFS_ENUM_PAIR {
                let cf_index: RaftStoreResult<usize> = snap
                    .cf_files()
                    .iter()
                    .position(|x| &x.cf == cf)
                    .ok_or(box_err!("can't find index for cf {}", cf));
                let cf_index = cf_index?;
                let cf_file = &snap.cf_files()[cf_index];
                let mut path = cf_file.path.clone();
                path.push(cf_file.file_prefix.clone());
                path.set_extension("sst");
                let mut _file = std::fs::File::create(path.as_path())?;
            }
            snap_data.set_region(new_region.clone());
            snap_data.set_file_size(0);
            const SNAPSHOT_VERSION: u64 = 2;
            snap_data.set_version(SNAPSHOT_VERSION);

            // SnapshotMeta
            // Which is snap.meta_file.meta
            let snapshot_meta = raftstore::store::snap::gen_snapshot_meta(snap.cf_files(), true)?;

            // Write MetaFile
            {
                let v = snapshot_meta.write_to_bytes()?;
                let mut f = std::fs::File::create(snap.meta_path())?;
                f.write_all(&v[..])?;
                f.flush()?;
                f.sync_all()?;
            }
            snap_data.set_meta(snapshot_meta);
        }

        // TODO The rest is test, please remove it after we can fetch the real data.
        pb_snapshot_metadata
            .set_conf_state(raftstore::store::util::conf_state_from_region(&new_region));
        pb_snapshot_metadata.set_index(key.idx);
        pb_snapshot_metadata.set_term(key.term);

        pb_snapshot.set_data(snap_data.write_to_bytes().unwrap().into());

        // Send reponse
        let mut response = RaftMessage::default();
        let epoch = new_region.get_region_epoch();
        response.set_region_epoch(epoch.clone());
        response.set_region_id(region_id);
        response.set_from_peer(msg.get_from_peer().clone());
        response.set_to_peer(msg.get_to_peer().clone());

        let message = response.mut_message();
        message.set_msg_type(MessageType::MsgSnapshot);
        message.set_term(inner_msg.get_term());
        message.set_snapshot(pb_snapshot);
        // If no set, will result in a MsgResponse to peer 0.
        message.set_from(msg.get_from_peer().get_id());
        message.set_to(msg.get_to_peer().get_id());
        debug!(
            "!!!! send snapshot to {} key {} raft message {:?} snap data {:?} apply_state {:?}",
            msg.get_to_peer().get_id(),
            key,
            response,
            snap_data,
            apply_state
        );
        match self.trans.lock() {
            Ok(mut trans) => match trans.send(response) {
                Ok(_) => {
                    let current = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap();
                    self.set_snapshot_inflight(region_id, current.as_millis())
                        .unwrap();
                }
                Err(RaftStoreError::RegionNotFound(_)) => (),
                _ => return Ok(crate::FastAddPeerStatus::OtherError),
            },
            Err(e) => return Err(box_err!("send snapshot meets error {:?}", e)),
        }

        Ok(crate::FastAddPeerStatus::Ok)
    }
}

impl<T: Transport + 'static, ER: RaftEngine> TiFlashObserver<T, ER> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store_id: u64,
        engine: engine_tiflash::RocksEngine,
        raft_engine: ER,
        sst_importer: Arc<SstImporter>,
        snap_handle_pool_size: usize,
        trans: T,
        snap_mgr: SnapManager,
        engine_store_cfg: crate::EngineStoreConfig,
    ) -> Self {
        let engine_store_server_helper =
            gen_engine_store_server_helper(engine.engine_store_server_helper);
        // start thread pool for pre handle snapshot
        let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
            .max_thread_count(snap_handle_pool_size)
            .build_future_pool();
        let mut cached_region_info = Vec::with_capacity(CACHED_REGION_INFO_SLOT_COUNT);
        for _ in 0..CACHED_REGION_INFO_SLOT_COUNT {
            cached_region_info.push(RwLock::new(HashMap::default()));
        }
        TiFlashObserver {
            store_id,
            engine_store_server_helper,
            engine,
            raft_engine,
            sst_importer,
            pre_handle_snapshot_ctx: Arc::new(Mutex::new(PrehandleContext::default())),
            snap_handle_pool_size,
            apply_snap_pool: Some(Arc::new(snap_pool)),
            pending_delete_ssts: Arc::new(RwLock::new(vec![])),
            cached_region_info: Arc::new(cached_region_info),
            trans: Arc::new(Mutex::new(trans)),
            snap_mgr: Arc::new(snap_mgr),
            engine_store_cfg,
        }
    }

    pub fn register_to<E: engine_traits::KvEngine>(
        &self,
        coprocessor_host: &mut CoprocessorHost<E>,
    ) {
        // If a observer is repeatedly registered, it can run repeated logic.
        coprocessor_host.registry.register_admin_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxAdminObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_query_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_apply_snapshot_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxApplySnapshotObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_region_change_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxRegionChangeObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_pd_task_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxPdTaskObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_update_safe_ts_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxUpdateSafeTsObserver::new(self.clone()),
        );
    }

    fn handle_ingest_sst_for_engine_store(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        ssts: &Vec<engine_traits::SstMetaInfo>,
        index: u64,
        term: u64,
    ) -> EngineStoreApplyRes {
        let mut ssts_wrap = vec![];
        let mut sst_views = vec![];

        info!("begin handle ingest sst";
            "region" => ?ob_ctx.region(),
            "index" => index,
            "term" => term,
        );

        for sst in ssts {
            let sst = &sst.meta;
            if sst.get_cf_name() == engine_traits::CF_LOCK {
                panic!("should not ingest sst of lock cf");
            }

            // We still need this to filter error ssts.
            if let Err(e) = check_sst_for_ingestion(sst, ob_ctx.region()) {
                error!(?e;
                 "proxy ingest fail";
                 "sst" => ?sst,
                 "region" => ?&ob_ctx.region(),
                );
                break;
            }

            ssts_wrap.push((
                self.sst_importer.get_path(sst),
                name_to_cf(sst.get_cf_name()),
            ));
        }

        for (path, cf) in &ssts_wrap {
            sst_views.push((path.to_str().unwrap().as_bytes(), *cf));
        }

        let res = self.engine_store_server_helper.handle_ingest_sst(
            sst_views,
            RaftCmdHeader::new(ob_ctx.region().get_id(), index, term),
        );
        res
    }

    fn handle_error_apply(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        region_state: &RegionState,
    ) -> bool {
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        let flash_res = self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
        );
        match flash_res {
            EngineStoreApplyRes::None => false,
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => false,
        }
    }
}

impl<T: Transport + 'static, ER: RaftEngine> Coprocessor for TiFlashObserver<T, ER> {
    fn stop(&self) {
        info!("shutdown tiflash observer"; "store_id" => self.store_id);
        self.apply_snap_pool.as_ref().unwrap().shutdown();
    }
}

impl<T: Transport + 'static, ER: RaftEngine> AdminObserver for TiFlashObserver<T, ER> {
    fn pre_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        req: &AdminRequest,
        index: u64,
        term: u64,
    ) -> bool {
        match req.get_cmd_type() {
            AdminCmdType::CompactLog => {
                if !self.engine_store_server_helper.try_flush_data(
                    ob_ctx.region().get_id(),
                    false,
                    false,
                    index,
                    term,
                ) {
                    info!("can't flush data, filter CompactLog";
                        "region_id" => ?ob_ctx.region().get_id(),
                        "region_epoch" => ?ob_ctx.region().get_region_epoch(),
                        "index" => index,
                        "term" => term,
                        "compact_index" => req.get_compact_log().get_compact_index(),
                        "compact_term" => req.get_compact_log().get_compact_term(),
                    );
                    return true;
                }
                // Otherwise, we can exec CompactLog, without later rolling
                // back.
            }
            AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                // We can't support.
                return true;
            }
            AdminCmdType::TransferLeader => {
                error!("transfer leader won't exec";
                        "region" => ?ob_ctx.region(),
                        "req" => ?req,
                );
                return true;
            }
            _ => (),
        };
        false
    }

    fn post_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        _: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        fail::fail_point!("on_post_exec_admin", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        let request = cmd.request.get_admin_request();
        let response = &cmd.response;
        let admin_reponse = response.get_admin_response();
        let cmd_type = request.get_cmd_type();

        if response.get_header().has_error() {
            info!(
                "error occurs when apply_admin_cmd, {:?}",
                response.get_header().get_error()
            );
            return self.handle_error_apply(ob_ctx, cmd, region_state);
        }

        match cmd_type {
            AdminCmdType::CompactLog | AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                info!(
                    "observe useless admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "type" => ?cmd_type,
                );
            }
            _ => {
                info!(
                    "observe admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "command" => ?request
                );
            }
        }

        // We wrap `modified_region` into `mut_split()`
        let mut new_response = None;
        match cmd_type {
            AdminCmdType::CommitMerge
            | AdminCmdType::PrepareMerge
            | AdminCmdType::RollbackMerge => {
                let mut r = AdminResponse::default();
                match region_state.modified_region.as_ref() {
                    Some(region) => r.mut_split().set_left(region.clone()),
                    None => {
                        error!("empty modified region";
                            "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id,
                            "term" => cmd.term,
                            "index" => cmd.index,
                            "command" => ?request
                        );
                        panic!("empty modified region");
                    }
                }
                new_response = Some(r);
            }
            _ => (),
        }

        let flash_res = {
            match new_response {
                Some(r) => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    &r,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                ),
                None => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    admin_reponse,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                ),
            }
        };
        let persist = match flash_res {
            EngineStoreApplyRes::None => {
                if cmd_type == AdminCmdType::CompactLog {
                    // This could only happen in mock-engine-store when we perform some related
                    // tests. Formal code should never return None for
                    // CompactLog now. If CompactLog can't be done, the
                    // engine-store should return `false` in previous `try_flush_data`.
                    error!("applying CompactLog should not return None"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "apply_state" => ?apply_state, "cmd" => ?cmd);
                }
                false
            }
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => {
                error!(
                    "region not found in engine-store, maybe have exec `RemoveNode` first";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                );
                !region_state.pending_remove
            }
        };
        if persist {
            info!("should persist admin"; "region_id" => ob_ctx.region().get_id(), "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }
}

impl<T: Transport + 'static, ER: RaftEngine> QueryObserver for TiFlashObserver<T, ER> {
    fn on_empty_cmd(&self, ob_ctx: &mut ObserverContext<'_>, index: u64, term: u64) {
        fail::fail_point!("on_empty_cmd_normal", |_| {});
        debug!("encounter empty cmd, maybe due to leadership change";
            "region" => ?ob_ctx.region(),
            "index" => index,
            "term" => term,
        );
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_ctx.region().get_id(), index, term),
        );
    }

    fn post_exec_query(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        apply_ctx_info: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        fail::fail_point!("on_post_exec_normal", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        const NONE_STR: &str = "";
        let requests = cmd.request.get_requests();
        let response = &cmd.response;
        if response.get_header().has_error() {
            let proto_err = response.get_header().get_error();
            if proto_err.has_flashback_in_progress() {
                debug!(
                    "error occurs when apply_write_cmd, {:?}",
                    response.get_header().get_error()
                );
            } else {
                info!(
                    "error occurs when apply_write_cmd, {:?}",
                    response.get_header().get_error()
                );
            }
            return self.handle_error_apply(ob_ctx, cmd, region_state);
        }

        let mut ssts = vec![];
        let mut cmds = WriteCmds::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => {
                    let put = req.get_put();
                    let cf = name_to_cf(put.get_cf());
                    let (key, value) = (put.get_key(), put.get_value());
                    cmds.push(key, value, WriteCmdType::Put, cf);
                }
                CmdType::Delete => {
                    let del = req.get_delete();
                    let cf = name_to_cf(del.get_cf());
                    let key = del.get_key();
                    cmds.push(key, NONE_STR.as_ref(), WriteCmdType::Del, cf);
                }
                CmdType::IngestSst => {
                    ssts.push(engine_traits::SstMetaInfo {
                        total_bytes: 0,
                        total_kvs: 0,
                        meta: req.get_ingest_sst().get_sst().clone(),
                    });
                }
                CmdType::Snap | CmdType::Get | CmdType::DeleteRange => {
                    // engine-store will drop table, no need DeleteRange
                    // We will filter delete range in engine_tiflash
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    panic!("invalid cmd type, message maybe corrupted");
                }
            }
        }

        let persist = if !ssts.is_empty() {
            assert_eq!(cmds.len(), 0);
            match self.handle_ingest_sst_for_engine_store(ob_ctx, &ssts, cmd.index, cmd.term) {
                EngineStoreApplyRes::None => {
                    // Before, BR/Lightning may let ingest sst cmd contain only one cf,
                    // which may cause that TiFlash can not flush all region cache into column.
                    // so we have a optimization proxy@cee1f003.
                    // The optimization is to introduce a `pending_delete_ssts`,
                    // which holds ssts from being cleaned(by adding into `delete_ssts`),
                    // when engine-store returns None.
                    // Though this is fixed by br#1150 & tikv#10202, we still have to handle None,
                    // since TiKV's compaction filter can also cause mismatch between default and
                    // write. According to tiflash#1811.
                    // Since returning None will cause no persistence of advanced apply index,
                    // So in a recovery, we can replay ingestion in `pending_delete_ssts`,
                    // thus leaving no un-tracked sst files.

                    // We must hereby move all ssts to `pending_delete_ssts` for protection.
                    match apply_ctx_info.pending_handle_ssts {
                        None => (), // No ssts to handle, unlikely.
                        Some(v) => {
                            self.pending_delete_ssts
                                .write()
                                .expect("lock error")
                                .append(v);
                        }
                    };
                    info!(
                        "skip persist for ingest sst";
                        "region_id" => ob_ctx.region().get_id(),
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                    );
                    false
                }
                EngineStoreApplyRes::NotFound | EngineStoreApplyRes::Persist => {
                    info!(
                        "ingest sst success";
                        "region_id" => ob_ctx.region().get_id(),
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                    );
                    match apply_ctx_info.pending_handle_ssts {
                        None => (),
                        Some(v) => {
                            let mut sst_in_region: Vec<SstMetaInfo> = self
                                .pending_delete_ssts
                                .write()
                                .expect("lock error")
                                .drain_filter(|e| {
                                    e.meta.get_region_id() == ob_ctx.region().get_id()
                                })
                                .collect();
                            apply_ctx_info.delete_ssts.append(&mut sst_in_region);
                            apply_ctx_info.delete_ssts.append(v);
                        }
                    }
                    !region_state.pending_remove
                }
            }
        } else {
            let flash_res = {
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                )
            };
            match flash_res {
                EngineStoreApplyRes::None => false,
                EngineStoreApplyRes::Persist => !region_state.pending_remove,
                EngineStoreApplyRes::NotFound => false,
            }
        };
        fail::fail_point!("on_post_exec_normal_end", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        if persist {
            info!("should persist query"; "region_id" => ob_ctx.region().get_id(), "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }
}

impl<T: Transport + 'static, ER: RaftEngine> UpdateSafeTsObserver for TiFlashObserver<T, ER> {
    fn on_update_safe_ts(&self, region_id: u64, self_safe_ts: u64, leader_safe_ts: u64) {
        self.engine_store_server_helper.handle_safe_ts_update(
            region_id,
            self_safe_ts,
            leader_safe_ts,
        )
    }
}

impl<T: Transport + 'static, ER: RaftEngine> RegionChangeObserver for TiFlashObserver<T, ER> {
    fn on_region_changed(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        e: RegionChangeEvent,
        _: StateRole,
    ) {
        let region_id = ob_ctx.region().get_id();
        if e == RegionChangeEvent::Destroy {
            info!(
                "observe destroy";
                "region_id" => region_id,
                "store_id" => self.store_id,
            );
            self.engine_store_server_helper
                .handle_destroy(ob_ctx.region().get_id());
            if self.engine_store_cfg.enable_fast_add_peer {
                self.remove_cached_region_info(region_id);
            }
        }
    }

    #[allow(clippy::match_like_matches_macro)]
    fn pre_persist(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        is_finished: bool,
        cmd: Option<&RaftCmdRequest>,
    ) -> bool {
        let should_persist = if is_finished {
            true
        } else {
            let cmd = cmd.unwrap();
            if cmd.has_admin_request() {
                match cmd.get_admin_request().get_cmd_type() {
                    // Merge needs to get the latest apply index.
                    AdminCmdType::CommitMerge | AdminCmdType::RollbackMerge => true,
                    _ => false,
                }
            } else {
                false
            }
        };
        if should_persist {
            debug!(
            "observe pre_persist, persist";
            "region_id" => ob_ctx.region().get_id(),
            "store_id" => self.store_id,
            );
        } else {
            debug!(
            "observe pre_persist";
            "region_id" => ob_ctx.region().get_id(),
            "store_id" => self.store_id,
            "is_finished" => is_finished,
            );
        };
        should_persist
    }

    fn pre_write_apply_state(&self, _ob_ctx: &mut ObserverContext<'_>) -> bool {
        fail::fail_point!("on_pre_persist_with_finish", |_| { true });
        false
    }

    fn should_skip_raft_message(&self, msg: &RaftMessage) -> bool {
        let inner_msg = msg.get_message();
        if inner_msg.get_commit() == 0 && inner_msg.get_msg_type() == MessageType::MsgHeartbeat {
        } else if inner_msg.get_msg_type() == MessageType::MsgAppend {
            return self.maybe_fast_path(&msg);
        }
        false
    }

    fn on_peer_created(&self, region_id: u64) {
        let f = |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
            MapEntry::Occupied(mut o) => {
                o.get_mut()
                    .replicated_or_created
                    .store(true, Ordering::SeqCst);
            }
            MapEntry::Vacant(v) => {
                let c = CachedRegionInfo::default();
                c.replicated_or_created.store(true, Ordering::SeqCst);
                v.insert(Arc::new(c));
            }
        };
        // TODO remove unwrap
        self.access_cached_region_info_mut(region_id, f).unwrap();
    }
}

impl<T: Transport + 'static, ER: RaftEngine> PdTaskObserver for TiFlashObserver<T, ER> {
    fn on_compute_engine_size(&self, store_size: &mut Option<StoreSizeInfo>) {
        let stats = self.engine_store_server_helper.handle_compute_store_stats();
        let _ = store_size.insert(StoreSizeInfo {
            capacity: stats.fs_stats.capacity_size,
            used: stats.fs_stats.used_size,
            avail: stats.fs_stats.avail_size,
        });
    }
}

fn retrieve_sst_files(snap: &store::Snapshot) -> Vec<(PathBuf, ColumnFamilyType)> {
    let mut sst_views: Vec<(PathBuf, ColumnFamilyType)> = vec![];
    let mut ssts = vec![];
    for cf_file in snap.cf_files() {
        // Skip empty cf file.
        // CfFile is changed by dynamic region.
        if cf_file.size.is_empty() {
            continue;
        }

        if cf_file.size[0] == 0 {
            continue;
        }

        if plain_file_used(cf_file.cf) {
            assert!(cf_file.cf == CF_LOCK);
        }
        // We have only one file for each cf for now.
        let mut full_paths = cf_file.file_paths();
        assert!(!full_paths.is_empty());
        if full_paths.len() != 1 {
            // Multi sst files for one cf.
            tikv_util::info!("observe multi-file snapshot";
                "snap" => ?snap,
                "cf" => ?cf_file.cf,
                "total" => full_paths.len(),
            );
            for f in full_paths.into_iter() {
                ssts.push((f, name_to_cf(cf_file.cf)));
            }
        } else {
            // Old case, one file for one cf.
            ssts.push((full_paths.remove(0), name_to_cf(cf_file.cf)));
        }
    }
    for (s, cf) in ssts.iter() {
        sst_views.push((PathBuf::from_str(s).unwrap(), *cf));
    }
    sst_views
}

fn pre_handle_snapshot_impl(
    engine_store_server_helper: &'static EngineStoreServerHelper,
    peer_id: u64,
    ssts: Vec<(PathBuf, ColumnFamilyType)>,
    region: &Region,
    snap_key: &SnapKey,
) -> PtrWrapper {
    let idx = snap_key.idx;
    let term = snap_key.term;
    let ptr = {
        let sst_views = ssts
            .iter()
            .map(|(b, c)| (b.to_str().unwrap().as_bytes(), c.clone()))
            .collect();
        engine_store_server_helper.pre_handle_snapshot(region, peer_id, sst_views, idx, term)
    };
    PtrWrapper(ptr)
}

impl<T: Transport + 'static, ER: RaftEngine> ApplySnapshotObserver for TiFlashObserver<T, ER> {
    #[allow(clippy::single_match)]
    fn pre_apply_snapshot(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        info!("pre apply snapshot";
            "peer_id" => peer_id,
            "region_id" => ob_ctx.region().get_id(),
            "snap_key" => ?snap_key,
            "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
        );
        fail::fail_point!("on_ob_pre_handle_snapshot", |_| {});

        let snap = match snap {
            None => return,
            Some(s) => s,
        };

        fail::fail_point!("on_ob_pre_handle_snapshot_delete", |_| {
            let ssts = retrieve_sst_files(snap);
            for (pathbuf, _) in ssts.iter() {
                debug!("delete snapshot file"; "path" => ?pathbuf);
                std::fs::remove_file(pathbuf.as_path()).unwrap();
            }
            return;
        });

        let (sender, receiver) = mpsc::channel();
        let task = Arc::new(PrehandleTask::new(receiver, peer_id));
        {
            let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                Ok(l) => l,
                Err(_) => fatal!("pre_apply_snapshot poisoned"),
            };
            let ctx = lock.deref_mut();
            ctx.tracer.insert(snap_key.clone(), task.clone());
        }

        let engine_store_server_helper = self.engine_store_server_helper;
        let region = ob_ctx.region().clone();
        let snap_key = snap_key.clone();
        let ssts = retrieve_sst_files(snap);
        match self.apply_snap_pool.as_ref() {
            Some(p) => {
                self.engine
                    .pending_applies_count
                    .fetch_add(1, Ordering::SeqCst);
                p.spawn(async move {
                    // The original implementation is in `Snapshot`, so we don't need to care abort
                    // lifetime.
                    fail::fail_point!("before_actually_pre_handle", |_| {});
                    let res = pre_handle_snapshot_impl(
                        engine_store_server_helper,
                        task.peer_id,
                        ssts,
                        &region,
                        &snap_key,
                    );
                    match sender.send(res) {
                        Err(_e) => error!("pre apply snapshot err when send to receiver"),
                        Ok(_) => (),
                    }
                });
            }
            None => {
                // quit background pre handling
                warn!("apply_snap_pool is not initialized";
                    "peer_id" => peer_id,
                    "region_id" => ob_ctx.region().get_id()
                );
            }
        }
    }

    fn post_apply_snapshot(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        fail::fail_point!("on_ob_post_apply_snapshot", |_| {
            return;
        });
        info!("post apply snapshot";
            "peer_id" => ?peer_id,
            "snap_key" => ?snap_key,
            "region" => ?ob_ctx.region(),
        );
        let region_id = ob_ctx.region().get_id();
        let mut should_skip = false;
        #[allow(clippy::collapsible_if)]
        if self.engine_store_cfg.enable_fast_add_peer {
            if self.access_cached_region_info_mut(
                region_id,
                |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                    MapEntry::Occupied(mut o) => {
                        let is_first_snapsot = !o.get().inited_or_fallback.load(Ordering::SeqCst);
                        if is_first_snapsot {
                            info!("fast path: applied first snapshot {}:{} {}, recover MsgAppend", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                            );
                            should_skip = true;
                            o.get_mut().snapshot_inflight.store(0, Ordering::SeqCst);
                            o.get_mut().inited_or_fallback.store(true, Ordering::SeqCst);
                        }
                    }
                    MapEntry::Vacant(_) => {
                        // Compat no fast add peer logic
                        // panic!("unknown snapshot!");
                    }
                },
            ).is_err() {
                fatal!("post_apply_snapshot poisoned")
            };
        }
        let snap = match snap {
            None => return,
            Some(s) => s,
        };
        let maybe_snapshot = {
            let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                Ok(l) => l,
                Err(_) => fatal!("post_apply_snapshot poisoned"),
            };
            let ctx = lock.deref_mut();
            ctx.tracer.remove(snap_key)
        };
        if should_skip {
            return;
        }
        let need_retry = match maybe_snapshot {
            Some(t) => {
                let neer_retry = match t.recv.recv() {
                    Ok(snap_ptr) => {
                        info!("get prehandled snapshot success";
                            "peer_id" => peer_id,
                            "snap_key" => ?snap_key,
                            "region_id" => ob_ctx.region().get_id(),
                            "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
                        );
                        if !should_skip {
                            self.engine_store_server_helper
                                .apply_pre_handled_snapshot(snap_ptr.0);
                        }
                        false
                    }
                    Err(_) => {
                        info!("background pre-handle snapshot get error";
                            "peer_id" => peer_id,
                            "snap_key" => ?snap_key,
                            "region_id" => ob_ctx.region().get_id(),
                            "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
                        );
                        true
                    }
                };
                self.engine
                    .pending_applies_count
                    .fetch_sub(1, Ordering::SeqCst);
                info!("apply snapshot finished";
                    "peer_id" => peer_id,
                    "snap_key" => ?snap_key,
                    "region" => ?ob_ctx.region(),
                    "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
                );
                neer_retry
            }
            None => {
                // We can't find background pre-handle task, maybe:
                // 1. we can't get snapshot from snap manager at that time.
                // 2. we disabled background pre handling.
                info!("pre-handled snapshot not found";
                    "peer_id" => peer_id,
                    "snap_key" => ?snap_key,
                    "region_id" => ob_ctx.region().get_id(),
                    "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
                );
                true
            }
        };
        if need_retry && !should_skip {
            let ssts = retrieve_sst_files(snap);
            let ptr = pre_handle_snapshot_impl(
                self.engine_store_server_helper,
                peer_id,
                ssts,
                ob_ctx.region(),
                snap_key,
            );
            info!("re-gen pre-handled snapshot success";
                "peer_id" => peer_id,
                "snap_key" => ?snap_key,
                "region_id" => ob_ctx.region().get_id(),
            );
            self.engine_store_server_helper
                .apply_pre_handled_snapshot(ptr.0);
            info!("apply snapshot finished";
                "peer_id" => peer_id,
                "snap_key" => ?snap_key,
                "region" => ?ob_ctx.region(),
                "pending" => self.engine.pending_applies_count.load(Ordering::SeqCst),
            );
        }
    }

    fn should_pre_apply_snapshot(&self) -> bool {
        true
    }
}
