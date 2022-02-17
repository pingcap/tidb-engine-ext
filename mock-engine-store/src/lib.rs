use engine_rocks::{Compat, RocksEngine, RocksSnapshot};
use engine_store_ffi::interfaces::root::DB as ffi_interfaces;
use engine_store_ffi::EngineStoreServerHelper;
use engine_store_ffi::RaftStoreProxyFFIHelper;
use engine_store_ffi::UnwrapExternCFunc;
use engine_traits::Peekable;
use engine_traits::{Engines, SyncMutable};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
};
use protobuf::Message;
use raftstore::engine_store_ffi;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::pin::Pin;
use tikv_util::{debug, error, info, warn};

type RegionId = u64;
#[derive(Default, Clone)]
pub struct Region {
    region: kvproto::metapb::Region,
    peer: kvproto::metapb::Peer, // What peer is me?
    data: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    apply_state: kvproto::raft_serverpb::RaftApplyState,
}

pub fn make_new_region(
    maybe_region: Option<kvproto::metapb::Region>,
    maybe_store_id: Option<u64>,
) -> Region {
    let mut region = Region {
        region: maybe_region.unwrap_or(Default::default()),
        ..Default::default()
    };
    if let Some(store_id) = maybe_store_id {
        set_new_region_peer(&mut region, store_id);
    }
    region
}

fn set_new_region_peer(new_region: &mut Region, store_id: u64) {
    if let Some(peer) = new_region
        .region
        .get_peers()
        .iter()
        .find(|&peer| peer.get_store_id() == store_id)
    {
        new_region.peer = peer.clone();
    } else {
        // This happens when region is not found.
    }
}

pub struct EngineStoreServer {
    pub id: u64,
    pub engines: Option<Engines<RocksEngine, RocksEngine>>,
    pub kvstore: HashMap<RegionId, Box<Region>>,
}

impl EngineStoreServer {
    pub fn new(id: u64, engines: Option<Engines<RocksEngine, RocksEngine>>) -> Self {
        // The first region is added in cluster.rs
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
        }
    }
}

pub struct EngineStoreServerWrap {
    pub engine_store_server: *mut EngineStoreServer,
    pub maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
    // Call `gen_cluster(cluster_ptr)`, and get which cluster this Server belong to.
    pub cluster_ptr: isize,
}

fn hacked_is_real_no_region(region_id: u64, engine_store_server: &mut EngineStoreServer) {
    if region_id == 1 {
        // In some tests, region 1 is not created on all nodes after store is started.
        // We need to double check rocksdb before we are sure there are no region 1.
        let kv = &mut engine_store_server.engines.as_mut().unwrap().kv;
        let local_state: Option<RegionLocalState> = kv
            .get_msg_cf(engine_traits::CF_RAFT, &keys::region_state_key(1))
            .unwrap_or(None);
        if local_state.is_none() {
            panic!("Can find region 1 in storage");
        }
        engine_store_server.kvstore.insert(
            region_id,
            Box::new(make_new_region(
                Some(local_state.unwrap().get_region().clone()),
                Some(engine_store_server.id),
            )),
        );
    }
}

impl EngineStoreServerWrap {
    pub fn new(
        engine_store_server: *mut EngineStoreServer,
        maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
        cluster_ptr: isize,
    ) -> Self {
        Self {
            engine_store_server,
            maybe_proxy_helper,
            cluster_ptr,
        }
    }

    unsafe fn handle_admin_raft_cmd(
        &mut self,
        req: &kvproto::raft_cmdpb::AdminRequest,
        resp: &kvproto::raft_cmdpb::AdminResponse,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        let node_id = (*self.engine_store_server).id;
        info!("handle admin raft cmd"; "request"=>?req, "response"=>?resp, "index"=>header.index, "region-id"=>header.region_id);
        let kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;
        let do_handle_admin_raft_cmd =
            move |region: &mut Region, engine_store_server: &mut EngineStoreServer| {
                if region.apply_state.get_applied_index() >= header.index {
                    return ffi_interfaces::EngineStoreApplyRes::Persist;
                }
                if req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::BatchSplit {
                    let regions = resp.get_splits().regions.as_ref();

                    for i in 0..regions.len() {
                        let region_meta = regions.get(i).unwrap();
                        if region_meta.id == region_id {
                            // This is the region to split from
                            assert!(engine_store_server.kvstore.contains_key(&region_meta.id));
                            engine_store_server
                                .kvstore
                                .get_mut(&region_meta.id)
                                .unwrap()
                                .region = region_meta.clone();
                        } else {
                            // Should split data into new region
                            let mut new_region =
                                make_new_region(Some(region_meta.clone()), Some(node_id));

                            debug!(
                                "new region {} generated by split at node {} with meta {:?}",
                                region_meta.id, node_id, region_meta
                            );
                            new_region
                                .apply_state
                                .mut_truncated_state()
                                .set_index(raftstore::store::RAFT_INIT_LOG_INDEX);
                            new_region
                                .apply_state
                                .mut_truncated_state()
                                .set_term(raftstore::store::RAFT_INIT_LOG_TERM);
                            new_region
                                .apply_state
                                .set_applied_index(raftstore::store::RAFT_INIT_LOG_INDEX);

                            // No need to split data because all KV are stored in the same RocksDB

                            // We can't assert `region_meta.id` is brand new here
                            engine_store_server
                                .kvstore
                                .insert(region_meta.id, Box::new(new_region));
                        }
                    }
                } else if req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::PrepareMerge {
                    let tikv_region = resp.get_split().get_left();

                    let target = req.prepare_merge.as_ref().unwrap().target.as_ref();
                    let region_meta = &mut (engine_store_server
                        .kvstore
                        .get_mut(&region_id)
                        .unwrap()
                        .region);
                    let region_epoch = region_meta.region_epoch.as_mut().unwrap();

                    let new_version = region_epoch.version + 1;
                    region_epoch.set_version(new_version);
                    assert_eq!(tikv_region.get_region_epoch().get_version(), new_version);

                    let conf_version = region_epoch.conf_ver + 1;
                    region_epoch.set_conf_ver(conf_version);
                    assert_eq!(tikv_region.get_region_epoch().get_conf_ver(), conf_version);

                    {
                        let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        region.apply_state.set_applied_index(header.index);
                    }
                    // We don't handle MergeState and PeerState here
                } else if req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::CommitMerge {
                    {
                        let tikv_region_meta = resp.get_split().get_left();

                        let target_region =
                            &mut (engine_store_server.kvstore.get_mut(&region_id).unwrap());
                        let target_region_meta = &mut target_region.region;
                        let target_version = target_region_meta.get_region_epoch().get_version();
                        let source_region = req.get_commit_merge().get_source();
                        let source_version = source_region.get_region_epoch().get_version();

                        let new_version = std::cmp::max(source_version, target_version) + 1;
                        target_region_meta
                            .mut_region_epoch()
                            .set_version(new_version);
                        assert_eq!(
                            target_region_meta.get_region_epoch().get_version(),
                            new_version
                        );

                        // No need to merge data
                        let source_at_left = if source_region.get_start_key().is_empty() {
                            true
                        } else if target_region_meta.get_start_key().is_empty() {
                            false
                        } else {
                            source_region
                                .get_end_key()
                                .cmp(target_region_meta.get_start_key())
                                == std::cmp::Ordering::Equal
                        };

                        if source_at_left {
                            target_region_meta
                                .set_start_key(source_region.get_start_key().to_vec());
                            assert_eq!(
                                tikv_region_meta.get_start_key(),
                                target_region_meta.get_start_key()
                            );
                        } else {
                            target_region_meta.set_end_key(source_region.get_end_key().to_vec());
                            assert_eq!(
                                tikv_region_meta.get_end_key(),
                                target_region_meta.get_end_key()
                            );
                        }

                        {
                            target_region.apply_state.set_applied_index(header.index);
                        }
                    }
                    {
                        engine_store_server
                            .kvstore
                            .remove(&req.get_commit_merge().get_source().get_id());
                    }
                } else if req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::RollbackMerge {
                    let region = (engine_store_server.kvstore.get_mut(&region_id).unwrap());
                    let region_meta = &mut region.region;
                    let new_version = region_meta.get_region_epoch().get_version() + 1;

                    region.apply_state.set_applied_index(header.index);
                } else if req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::ChangePeer
                    || req.cmd_type == kvproto::raft_cmdpb::AdminCmdType::ChangePeerV2
                {
                    let new_region_meta = resp.get_change_peer().get_region();

                    let old_peer_id = {
                        let old_region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        old_region.region = new_region_meta.clone();
                        old_region.apply_state.set_applied_index(header.index);
                        old_region.peer.get_id()
                    };

                    let mut do_remove = true;
                    for peer in new_region_meta.get_peers() {
                        if peer.get_id() == old_peer_id {
                            // Should not remove region
                            do_remove = false;
                        }
                    }
                    if do_remove {
                        let removed = engine_store_server.kvstore.remove(&region_id);
                        // We need to also remove apply state, thus we need to know peer_id
                        debug!(
                            "Remove region {:?} peer_id {} at node {}",
                            removed.unwrap().region,
                            old_peer_id,
                            node_id
                        );
                    }
                } else if [
                    kvproto::raft_cmdpb::AdminCmdType::CompactLog,
                    kvproto::raft_cmdpb::AdminCmdType::ComputeHash,
                    kvproto::raft_cmdpb::AdminCmdType::VerifyHash,
                ]
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<kvproto::raft_cmdpb::AdminCmdType>>()
                .contains(&req.cmd_type)
                {
                    let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                    region.apply_state.set_applied_index(header.index);
                }
                ffi_interfaces::EngineStoreApplyRes::Persist
            };
        if !(*self.engine_store_server).kvstore.contains_key(&region_id) {
            hacked_is_real_no_region(region_id, &mut *self.engine_store_server);
        }
        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_admin_raft_cmd(o.get_mut(), &mut (*self.engine_store_server))
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!(
                    "handle_admin_raft_cmd region {} not found at node {}",
                    region_id, node_id
                );

                // do_handle_admin_raft_cmd(
                //     v.insert(Box::new(make_new_region(None, Some(node_id)))),
                //     &mut (*self.engine_store_server),
                // )
                ffi_interfaces::EngineStoreApplyRes::NotFound
            }
        }
    }

    unsafe fn handle_write_raft_cmd(
        &mut self,
        cmds: ffi_interfaces::WriteCmdsView,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        let node_id = (*self.engine_store_server).id;
        let server = &mut (*self.engine_store_server);
        let kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;
        let mut do_handle_write_raft_cmd = move |region: &mut Region| {
            if region.apply_state.get_applied_index() >= header.index {
                debug!("handle_write_raft_cmd meet old index");
                return ffi_interfaces::EngineStoreApplyRes::None;
            }
            debug!(
                "handle_write_raft_cmd region {} node id {}",
                region_id, server.id,
            );
            for i in 0..cmds.len {
                let key = &*cmds.keys.add(i as _);
                let val = &*cmds.vals.add(i as _);
                debug!(
                    "handle_write_raft_cmd add K {:?} V {:?}",
                    key.to_slice(),
                    val.to_slice(),
                );
                let tp = &*cmds.cmd_types.add(i as _);
                let cf = &*cmds.cmd_cf.add(i as _);
                let cf_index = (*cf) as u8;
                let data = &mut region.data[cf_index as usize];
                match tp {
                    engine_store_ffi::WriteCmdType::Put => {
                        let tikv_key = keys::data_key(key.to_slice());
                        kv.put_cf(
                            cf_to_name(cf.to_owned().into()),
                            &tikv_key,
                            &val.to_slice().to_vec(),
                        )
                        .map_err(std::convert::identity);
                    }
                    engine_store_ffi::WriteCmdType::Del => {
                        let tikv_key = keys::data_key(key.to_slice());
                        kv.delete_cf(cf_to_name(cf.to_owned().into()), &tikv_key);
                    }
                }
            }
            region.apply_state.set_applied_index(header.index);
            persist_apply_state(
                region,
                kv,
                region_id,
                true,
                false,
                header.index,
                header.term,
            );
            // Do not advance apply index
            ffi_interfaces::EngineStoreApplyRes::None
        };

        if !(*self.engine_store_server).kvstore.contains_key(&region_id) {
            hacked_is_real_no_region(region_id, &mut *self.engine_store_server);
        }
        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_write_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!(
                    "handle_write_raft_cmd region {} not found at node {}",
                    region_id, node_id
                );
                // do_handle_write_raft_cmd(v.insert(Box::new(make_new_region(None, Some(node_id)))))
                ffi_interfaces::EngineStoreApplyRes::NotFound
            }
        }
    }
}

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
    EngineStoreServerHelper {
        magic_number: ffi_interfaces::RAFT_STORE_PROXY_MAGIC_NUMBER,
        version: ffi_interfaces::RAFT_STORE_PROXY_VERSION,
        inner: &(*wrap) as *const EngineStoreServerWrap as *mut _,
        fn_gen_cpp_string: Some(ffi_gen_cpp_string),
        fn_handle_write_raft_cmd: Some(ffi_handle_write_raft_cmd),
        fn_handle_admin_raft_cmd: Some(ffi_handle_admin_raft_cmd),
        fn_atomic_update_proxy: Some(ffi_atomic_update_proxy),
        fn_handle_destroy: Some(ffi_handle_destroy),
        fn_handle_ingest_sst: Some(ffi_handle_ingest_sst),
        fn_handle_compute_store_stats: Some(ffi_handle_compute_store_stats),
        fn_handle_get_engine_store_server_status: None,
        fn_pre_handle_snapshot: Some(ffi_pre_handle_snapshot),
        fn_apply_pre_handled_snapshot: Some(ffi_apply_pre_handled_snapshot),
        fn_handle_http_request: None,
        fn_check_http_uri_available: None,
        fn_gc_raw_cpp_ptr: Some(ffi_gc_raw_cpp_ptr),
        fn_insert_batch_read_index_resp: None,
        fn_set_server_info_resp: None,
        fn_get_config: None,
        fn_set_store: None,
    }
}

unsafe fn into_engine_store_server_wrap(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
) -> &'static mut EngineStoreServerWrap {
    &mut *(arg1 as *mut EngineStoreServerWrap)
}

unsafe extern "C" fn ffi_handle_admin_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::BaseBuffView,
    arg3: ffi_interfaces::BaseBuffView,
    arg4: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let mut req = kvproto::raft_cmdpb::AdminRequest::default();
    let mut resp = kvproto::raft_cmdpb::AdminResponse::default();
    req.merge_from_bytes(arg2.to_slice()).unwrap();
    resp.merge_from_bytes(arg3.to_slice()).unwrap();
    store.handle_admin_raft_cmd(&req, &resp, arg4)
}

unsafe extern "C" fn ffi_handle_write_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::WriteCmdsView,
    arg3: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    store.handle_write_raft_cmd(arg2, arg3)
}

enum RawCppPtrTypeImpl {
    None = 0,
    String,
    PreHandledSnapshotWithBlock,
}

impl From<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn from(o: ffi_interfaces::RawCppPtrType) -> Self {
        match o {
            0 => RawCppPtrTypeImpl::None,
            1 => RawCppPtrTypeImpl::String,
            2 => RawCppPtrTypeImpl::PreHandledSnapshotWithBlock,
            _ => unreachable!(),
        }
    }
}

impl Into<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn into(self) -> ffi_interfaces::RawCppPtrType {
        match self {
            RawCppPtrTypeImpl::None => 0,
            RawCppPtrTypeImpl::String => 1,
            RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => 2,
        }
    }
}

#[no_mangle]
extern "C" fn ffi_gen_cpp_string(s: ffi_interfaces::BaseBuffView) -> ffi_interfaces::RawCppPtr {
    let str = Box::new(Vec::from(s.to_slice()));
    let ptr = Box::into_raw(str);
    ffi_interfaces::RawCppPtr {
        ptr: ptr as *mut _,
        type_: RawCppPtrTypeImpl::String.into(),
    }
}

#[no_mangle]
extern "C" fn ffi_gc_raw_cpp_ptr(
    ptr: ffi_interfaces::RawVoidPtr,
    tp: ffi_interfaces::RawCppPtrType,
) {
    match RawCppPtrTypeImpl::from(tp) {
        RawCppPtrTypeImpl::None => {}
        RawCppPtrTypeImpl::String => unsafe {
            Box::<Vec<u8>>::from_raw(ptr as *mut _);
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => unsafe {
            Box::<PrehandledSnapshot>::from_raw(ptr as *mut _);
        },
    }
}

unsafe extern "C" fn ffi_atomic_update_proxy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: *mut ffi_interfaces::RaftStoreProxyFFIHelper,
) {
    let store = into_engine_store_server_wrap(arg1);
    store.maybe_proxy_helper = Some(&mut *(arg2 as *mut RaftStoreProxyFFIHelper));
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    (*store.engine_store_server).kvstore.remove(&arg2);
}

type TiFlashRaftProxyHelper = RaftStoreProxyFFIHelper;

pub struct SSTReader<'a> {
    proxy_helper: &'a TiFlashRaftProxyHelper,
    inner: ffi_interfaces::SSTReaderPtr,
    type_: ffi_interfaces::ColumnFamilyType,
}

impl<'a> Drop for SSTReader<'a> {
    fn drop(&mut self) {
        unsafe {
            (self.proxy_helper.sst_reader_interfaces.fn_gc.into_inner())(
                self.inner.clone(),
                self.type_,
            );
        }
    }
}

impl<'a> SSTReader<'a> {
    pub unsafe fn new(
        proxy_helper: &'a TiFlashRaftProxyHelper,
        view: &'a ffi_interfaces::SSTView,
    ) -> Self {
        SSTReader {
            proxy_helper,
            inner: (proxy_helper
                .sst_reader_interfaces
                .fn_get_sst_reader
                .into_inner())(view.clone(), proxy_helper.proxy_ptr.clone()),
            type_: view.type_,
        }
    }

    pub unsafe fn remained(&mut self) -> bool {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_remained
            .into_inner())(self.inner.clone(), self.type_)
            != 0
    }

    pub unsafe fn key(&mut self) -> ffi_interfaces::BaseBuffView {
        (self.proxy_helper.sst_reader_interfaces.fn_key.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }

    pub unsafe fn value(&mut self) -> ffi_interfaces::BaseBuffView {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_value
            .into_inner())(self.inner.clone(), self.type_)
    }

    pub unsafe fn next(&mut self) {
        (self.proxy_helper.sst_reader_interfaces.fn_next.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }
}

struct PrehandledSnapshot {
    pub region: std::option::Option<Region>,
}

unsafe extern "C" fn ffi_pre_handle_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    region_buff: ffi_interfaces::BaseBuffView,
    peer_id: u64,
    snaps: ffi_interfaces::SSTViewVec,
    index: u64,
    term: u64,
) -> ffi_interfaces::RawCppPtr {
    let store = into_engine_store_server_wrap(arg1);
    let node_id = (*store.engine_store_server).id;
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    let kvstore = &mut (*store.engine_store_server).kvstore;

    let mut region_meta = kvproto::metapb::Region::default();
    assert_ne!(region_buff.data, std::ptr::null());
    assert_ne!(region_buff.len, 0);
    region_meta
        .merge_from_bytes(region_buff.to_slice())
        .unwrap();

    let mut region = make_new_region(Some(region_meta), Some(node_id));

    debug!(
        "prehandle snapshot with len {} node_id {} peer_id {}",
        snaps.len, node_id, peer_id
    );
    for i in 0..snaps.len {
        let mut snapshot = snaps.views.add(i as usize);
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut ffi_interfaces::SSTView));

        {
            region.apply_state.mut_truncated_state().set_index(index);
            region.apply_state.mut_truncated_state().set_term(term);
            {
                region.apply_state.set_applied_index(index);
            }
        }

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();

            let cf_index = (*snapshot).type_ as usize;
            let data = &mut region.data[cf_index];
            let _ = data.insert(key.to_slice().to_vec(), value.to_slice().to_vec());

            sst_reader.next();
        }
    }

    ffi_interfaces::RawCppPtr {
        ptr: Box::into_raw(Box::new(PrehandledSnapshot {
            region: Some(region),
        })) as *const Region as ffi_interfaces::RawVoidPtr,
        type_: RawCppPtrTypeImpl::PreHandledSnapshotWithBlock.into(),
    }
}

pub fn cf_to_name(cf: ffi_interfaces::ColumnFamilyType) -> &'static str {
    match cf {
        ffi_interfaces::ColumnFamilyType::Lock => CF_LOCK,
        ffi_interfaces::ColumnFamilyType::Write => CF_WRITE,
        ffi_interfaces::ColumnFamilyType::Default => CF_DEFAULT,
    }
}

unsafe extern "C" fn ffi_apply_pre_handled_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::RawVoidPtr,
    arg3: ffi_interfaces::RawCppPtrType,
) {
    let store = into_engine_store_server_wrap(arg1);
    let req = &mut *(arg2 as *mut PrehandledSnapshot);
    let node_id = (*store.engine_store_server).id;

    let req_id = req.region.as_ref().unwrap().region.id;

    // Though we do not write to kvstore in memory now, we still need to maintain regions.
    &(*store.engine_store_server)
        .kvstore
        .insert(req_id, Box::new(req.region.take().unwrap()));

    let region = (*store.engine_store_server)
        .kvstore
        .get_mut(&req_id)
        .unwrap();

    debug!(
        "apply pre-handled snapshot on new_region {} at store {}",
        req_id, node_id
    );

    let kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;
    for cf in 0..3 {
        for (k, v) in std::mem::take(region.data.as_mut().get_mut(cf).unwrap()).into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            kv.put_cf(cf_name, &tikv_key, &v)
                .map_err(std::convert::identity);
        }
    }
}

unsafe extern "C" fn ffi_handle_ingest_sst(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    snaps: ffi_interfaces::SSTViewVec,
    header: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    debug!("ingest sst with len {}", snaps.len);

    let region_id = header.region_id;
    let kvstore = &mut (*store.engine_store_server).kvstore;
    let kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;
    let region = kvstore.get_mut(&region_id).unwrap();

    for i in 0..snaps.len {
        let snapshot = snaps.views.add(i as usize);
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut ffi_interfaces::SSTView));

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();
            let tikv_key = keys::data_key(key.to_slice());
            let cf_name = cf_to_name((*snapshot).type_);
            kv.put_cf(cf_name, &tikv_key, &value.to_slice())
                .map_err(std::convert::identity);
            sst_reader.next();
        }
    }

    // Since tics#1811, Br/Lightning will always ingest both WRITE and DEFAULT, so we can always persist, rather than wait.
    ffi_interfaces::EngineStoreApplyRes::Persist
}

fn persist_apply_state(
    region: &mut Region,
    kv: &mut RocksEngine,
    region_id: u64,
    persist_apply_index: bool,
    persist_truncated_state: bool,
    potential_index: u64,
    potential_term: u64,
) {
    let apply_key = keys::apply_state_key(region_id);
    let mut old_apply_state = kv
        .get_msg_cf::<RaftApplyState>(engine_traits::CF_RAFT, &apply_key)
        .unwrap_or(None);
    if old_apply_state.is_none() {
        // Have not set apply_state, use ours
        kv.put_cf(
            engine_traits::CF_RAFT,
            &apply_key,
            &region.apply_state.write_to_bytes().unwrap(),
        )
        .map_err(std::convert::identity);
    } else {
        let old_apply_state = old_apply_state.as_mut().unwrap();
        if persist_apply_index {
            old_apply_state.set_applied_index(region.apply_state.get_applied_index());
            if potential_index > old_apply_state.get_commit_index()
                || potential_term > old_apply_state.get_commit_term()
            {
                old_apply_state.set_commit_index(potential_index);
                old_apply_state.set_commit_term(potential_term);
                region.apply_state.set_commit_index(potential_index);
                region.apply_state.set_commit_term(potential_term);
            }
        }
        if persist_truncated_state {
            old_apply_state
                .mut_truncated_state()
                .set_index(region.apply_state.get_truncated_state().get_index());
            old_apply_state
                .mut_truncated_state()
                .set_term(region.apply_state.get_truncated_state().get_term());
        }
        if persist_apply_index || persist_truncated_state {
            kv.put_cf(
                engine_traits::CF_RAFT,
                &apply_key,
                &old_apply_state.write_to_bytes().unwrap(),
            )
            .map_err(std::convert::identity);
        }
    }
}

unsafe extern "C" fn ffi_handle_compute_store_stats(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
) -> ffi_interfaces::StoreStats {
    ffi_interfaces::StoreStats {
        fs_stats: ffi_interfaces::FsStats {
            used_size: 0,
            avail_size: 0,
            capacity_size: 0,
            ok: 1,
        },
        engine_bytes_written: 0,
        engine_keys_written: 0,
        engine_bytes_read: 0,
        engine_keys_read: 0,
    }
}

unsafe impl Sync for EngineStoreServer {}
unsafe impl Sync for EngineStoreServerWrap {}
