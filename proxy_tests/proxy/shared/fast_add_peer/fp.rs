// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::hash_map::Entry as MapEntry;

use engine_tiflash::CachedRegionInfo;
use engine_traits::SyncMutable;

use super::must_wait_fap_snapshot_removed;
use crate::utils::v1::*;

fn setup_fp_for_fap() {
    fail::cfg("post_apply_snapshot_allow_no_unips", "return").unwrap();
    fail::cfg("on_apply_snapshot_committed_allow_no_unips", "return").unwrap();
}

fn must_wait_fap_snapshot_applying(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_id: u64,
    peer_id: u64,
) {
    // Temporary FAP data is visible before the synthetic Raft snapshot is sent.
    // Wait until the receiver persists Applying before interrupting its lifecycle.
    must_wait_until_cond_generic(cluster_ext, region_id, Some(vec![store_id]), &|_| {
        let mut ready = false;
        cluster_ext.iter_ffi_helpers(Some(vec![store_id]), &mut |_, ffi: &mut FFIHelperSet| {
            let fap_state = ffi
                .engine_store_server_helper
                .query_fap_snapshot_state(region_id, peer_id, 0, 0);
            let engine = &ffi.engine_store_server.engines.as_ref().unwrap().kv;
            let region_state = general_get_region_local_state(engine, region_id);
            ready = fap_state == proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
                && region_state.map_or(false, |state| state.get_state() == PeerState::Applying);
        });
        ready
    });
}

#[test]
fn test_disable_fap() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    fail::cfg("fap_core_fake_send", "return(1)").unwrap(); // Can always apply snapshot immediately
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    check_key(&cluster, b"k0", b"v0", Some(true), None, Some(vec![1]));

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    std::thread::sleep(std::time::Duration::from_millis(1000));

    stop_tiflash_node(&mut cluster, 2);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = false;

    fail::cfg("fap_core_no_fast_path", "panic").unwrap();
    for i in 10..19 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }
    for i in 10..19 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            None,
            Some(vec![1]),
        );
    }
    let region = cluster.get_region("k19".as_bytes());
    let prev_state = maybe_collect_states(&cluster.cluster_ext, 1, Some(vec![1]));
    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    debug!("compact at index {}", compact_index);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(1, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    // compact index should less than applied index
    assert!(!res.get_header().has_error(), "{:?}", res);

    restart_tiflash_node(&mut cluster, 2);
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    // The original fap snapshot not match
    check_key(&cluster, b"k0", b"v0", Some(true), None, Some(vec![2]));
    check_key(&cluster, b"k10", b"v10", Some(true), None, Some(vec![2]));

    cluster.shutdown();
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
}

#[test]
fn test_cancel_after_fap_phase1() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    fail::cfg("on_ob_cancel_after_pre_handle_snapshot", "return").unwrap();
    fail::cfg("on_ob_post_apply_snapshot", "pause").unwrap();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    std::thread::sleep(std::time::Duration::from_millis(2000));

    // We don't clear fap snapshot(if any) when canceled.
    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        let r = ffi
            .engine_store_server
            .engines
            .as_ref()
            .unwrap()
            .kv
            .proxy_ext
            .cached_region_info_manager
            .as_ref();
        assert!(r.is_some());
        assert!(r.unwrap().contains(1));
    });

    fail::remove("on_ob_post_apply_snapshot");

    cluster.shutdown();
    fail::remove("on_ob_cancel_after_pre_handle_snapshot");
    fail::remove("fap_core_no_fallback");
}

// Test is the cached mem info is well managed in all cases.
#[test]
fn test_restart_meta_info() {
    setup_fp_for_fap();
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    stop_tiflash_node(&mut cluster, 1);
    restart_tiflash_node(&mut cluster, 1);

    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));
    iter_ffi_helpers(
        &cluster,
        Some(vec![1, 2]),
        &mut |_, ffi: &mut FFIHelperSet| {
            let r = ffi
                .engine_store_server
                .engines
                .as_ref()
                .unwrap()
                .kv
                .proxy_ext
                .cached_region_info_manager
                .as_ref();
            assert!(r.is_some());
            assert!(r.unwrap().contains(1));
        },
    );

    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1, 2]));

    cluster.must_split(&cluster.get_region(b"k1"), b"k2");
    let r1_id = cluster.get_region_id(b"k1");
    let r3_id = cluster.get_region_id(b"k3");
    iter_ffi_helpers(
        &cluster,
        Some(vec![1, 2]),
        &mut |_, ffi: &mut FFIHelperSet| {
            let r = ffi
                .engine_store_server
                .engines
                .as_ref()
                .unwrap()
                .kv
                .proxy_ext
                .cached_region_info_manager
                .as_ref();
            assert!(r.is_some());
            assert!(r.unwrap().contains(r1_id));
            assert!(r.unwrap().contains(r3_id));
        },
    );

    cluster.shutdown();
    fail::remove("post_apply_snapshot_allow_no_unips");
    fail::remove("fap_core_no_fallback");
}

fn prehandle_snapshot_after_restart(kind: u64) {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    disable_auto_gen_compact_log(&mut cluster);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    fail::cfg("fap_mock_add_peer_from_id", "return(4)").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2002));
    check_key(&cluster, b"k1", b"v1", None, Some(true), Some(vec![2]));
    check_key(&cluster, b"k3", b"v3", None, Some(true), Some(vec![2]));

    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();

    // Pause before adding the peer so snapshot application cannot race the
    // simulated restart below.
    fail::cfg("on_ob_pre_handle_snapshot_s3", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3003));

    must_wait_fap_snapshot_applying(&cluster.cluster_ext, 1, 3, 3003);
    // Mock cached region info manager is cleared.
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        if kind == 1 {
            // So it will `fast path: check should apply fap snapshot noexist`
            ffi.engine_store_server
                .engines
                .as_ref()
                .unwrap()
                .kv
                .proxy_ext
                .cached_region_info_manager
                .as_ref()
                .unwrap()
                .clear();
            assert!(
                !ffi.engine_store_server
                    .engines
                    .as_ref()
                    .unwrap()
                    .kv
                    .proxy_ext
                    .cached_region_info_manager
                    .as_ref()
                    .unwrap()
                    .contains(1)
            );
        } else {
            // Set inited_or_fallback to false.
            ffi.engine_store_server
                .engines
                .as_ref()
                .unwrap()
                .kv
                .proxy_ext
                .cached_region_info_manager
                .as_ref()
                .unwrap()
                .access_cached_region_info_mut(1, |info: MapEntry<u64, Arc<CachedRegionInfo>>| {
                    match info {
                        MapEntry::Occupied(o) => {
                            o.get().inited_or_fallback.store(false, Ordering::SeqCst);
                            o.get().snapshot_inflight.store(0, Ordering::SeqCst);
                        }
                        MapEntry::Vacant(_) => {
                            tikv_util::safe_panic!("panicked");
                        }
                    }
                })
                .unwrap();
        }
    });

    // Under this case, still reuse the fap snapshot.
    fail::cfg("fap_core_no_prehandle", "panic").unwrap();
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 3003, 0, 0),
            proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
        );
    });
    fail::remove("on_ob_pre_handle_snapshot_s3");

    // And it can be deleted.
    check_key(&cluster, b"k3", b"v3", None, Some(true), Some(vec![3]));
    cluster.shutdown();

    fail::remove("post_apply_snapshot_allow_no_unips");
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("fap_core_no_prehandle");
}

#[test]
fn test_prehandle_snapshot_after_restart_reset() {
    prehandle_snapshot_after_restart(2);
}

#[test]
fn test_prehandle_snapshot_after_restart_1() {
    prehandle_snapshot_after_restart(1);
}

// The idea is:
// - old_one is replicated to store 3 as a normal raft snapshot. It has the
//   original wider range.
// - new_one is derived from old_one, and then replicated to store 2 by normal
//   path, and then replicated to store 3 by FAP.

// Expected result is:
// - apply snapshot old_one [-inf, inf)
// - pre handle old_one [-inf, inf)
// - fap handle new_one [-inf, k2)
//      - pre-handle data
//      - ingest data(the post apply stage on TiFlash)
//      - send fake and empty snapshot
// - apply snapshot new_one [-inf, k2) <- won't happen, due to overlap
// - post apply new_one [-inf, k2), k1=v13 <- won't happen
// - post apply old_one [-inf, inf), k1=v1

#[test]
fn test_overlap_last_apply_old() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    disable_auto_gen_compact_log(&mut cluster);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // Use an invalid store id to make FAP fallback.
    fail::cfg("fap_mock_add_peer_from_id", "return(4)").unwrap();

    // Delay, so the tikv snapshot comes after fap snapshot in pending_applies
    // queue.
    fail::cfg("on_ob_pre_handle_snapshot_s3", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3003));
    std::thread::sleep(std::time::Duration::from_millis(1000));

    debug!("prepare split");
    // Split
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));
    // Generates 2 peers {1001@1, 1002@3} for region 1.
    // However, we use the older snapshot, so the 1002 peer is not inited.
    cluster.must_split(&cluster.get_region(b"k1"), b"k2");

    debug!("prepare remove peer");
    let new_one_1000_k1 = cluster.get_region(b"k1");
    let old_one_1_k3 = cluster.get_region(b"k3"); // region_id = 1
    assert_ne!(new_one_1000_k1.get_id(), old_one_1_k3.get_id());
    pd_client.must_remove_peer(new_one_1000_k1.get_id(), new_learner_peer(3, 1002));
    assert_ne!(new_one_1000_k1.get_id(), old_one_1_k3.get_id());
    assert_eq!(1, old_one_1_k3.get_id());

    // Prevent FAP
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
    debug!(
        "old_one(with k3) is {}, new_one(with k1) is {}",
        old_one_1_k3.get_id(),
        new_one_1000_k1.get_id()
    );
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        old_one_1_k3.get_id(),
        Some(vec![1]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 2
        },
    );

    // k1 was in old region, but reassigned to new region then.
    cluster.must_put(b"k1", b"v13");
    std::thread::sleep(std::time::Duration::from_millis(1000));

    // Prepare a peer for FAP.
    pd_client.must_add_peer(new_one_1000_k1.get_id(), new_learner_peer(2, 2003));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        new_one_1000_k1.get_id(),
        Some(vec![1, 2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 2
        },
    );

    fail::cfg("on_can_apply_snapshot", "return(false)").unwrap();
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
    // FAP will ingest data, but not finish applying snapshot due to failpoint.
    pd_client.must_add_peer(new_one_1000_k1.get_id(), new_learner_peer(3, 3001));
    // TODO wait FAP finished "build and send"
    std::thread::sleep(std::time::Duration::from_millis(5000));
    // Now let store's snapshot of region 1 to prehandle.
    // So it will come after 1003 in `pending_applies`.
    fail::remove("on_ob_pre_handle_snapshot_s3");
    std::thread::sleep(std::time::Duration::from_millis(1000));

    // Reject all raft log, to test snapshot result.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));

    fail::remove("on_can_apply_snapshot");
    debug!("remove on_can_apply_snapshot");

    must_not_wait_until_cond_generic_for(
        &cluster.cluster_ext,
        new_one_1000_k1.get_id(),
        Some(vec![3]),
        &|states: &HashMap<u64, States>| -> bool { states.contains_key(&1000) },
        3000,
    );

    // k1 is in a different region in store 3 than in global view.
    assert_eq!(cluster.get_region(b"k1").get_id(), new_one_1000_k1.get_id());
    check_key(&cluster, b"k1", b"v1", None, Some(true), Some(vec![3]));
    check_key_ex(
        &cluster,
        b"k1",
        b"v1",
        Some(true),
        None,
        Some(vec![3]),
        Some(old_one_1_k3.get_id()),
        true,
    );
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![3]));

    cluster.clear_send_filters();

    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("on_can_apply_snapshot");
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}

// If a tikv snapshot is applied between fn_fast_add_peer and
// build_and_send_snapshot, it will override the previous snapshot's data, which
// is actually newer.
// It if origianlly https://github.com/pingcap/tidb-engine-ext/pull/359 before two-stage fap.
#[test]
fn test_overlap_apply_tikv_snap_in_the_middle() {
    const SPLIT_REGION_ID: u64 = 1000;
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    disable_auto_gen_compact_log(&mut cluster);
    fail::cfg("fap_core_fallback_millis", "return(0)").unwrap();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    cluster.cfg.tikv.raft_store.store_batch_system.pool_size = 4;
    cluster.cfg.tikv.raft_store.apply_batch_system.pool_size = 4;
    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // Only node 1's ordinary snapshots to store 3 are blocked. The synthetic
    // FAP snapshot uses store 3's transport and must remain deliverable.
    cluster.sim.write().unwrap().add_send_filter(
        1,
        Box::new(
            RegionPacketFilter::new(SPLIT_REGION_ID, 3)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgSnapshot),
        ),
    );

    // Use an invalid store id to make FAP fallback.
    fail::cfg("fap_mock_add_peer_from_id", "return(4)").unwrap();

    // Don't use send filter to prevent applying snapshot,
    // since it may no longer send snapshot after split.
    let (old_snapshot_tx, old_snapshot_rx) = mpsc::sync_channel(1);
    let (resume_old_tx, resume_old_rx) = mpsc::sync_channel(1);
    let resume_old_rx = std::sync::Mutex::new(resume_old_rx);
    fail::cfg_callback("fap_on_msg_snapshot_1_3003", move || {
        old_snapshot_tx.send(()).unwrap();
        resume_old_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .expect("old snapshot was not released");
    })
    .unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3003));
    old_snapshot_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("old snapshot did not arrive before split");

    // Split
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));
    // Generates 2 peers {1001@1, 1002@3} for region 1.
    // However, we use the older snapshot, so the 1002 peer is not inited.
    cluster.must_split(&cluster.get_region(b"k1"), b"k2");

    let new_one_1000_k1 = cluster.get_region(b"k1");
    let old_one_1_k3 = cluster.get_region(b"k3"); // region_id = 1
    assert_eq!(new_one_1000_k1.get_id(), SPLIT_REGION_ID);
    assert_ne!(new_one_1000_k1.get_id(), old_one_1_k3.get_id());
    pd_client.must_remove_peer(new_one_1000_k1.get_id(), new_learner_peer(3, 1002));
    assert_ne!(new_one_1000_k1.get_id(), old_one_1_k3.get_id());
    assert_eq!(1, old_one_1_k3.get_id());

    // Prevent FAP
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
    debug!(
        "old_one(with k3) is {}, new_one(with k1) is {}",
        old_one_1_k3.get_id(),
        new_one_1000_k1.get_id()
    );
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        old_one_1_k3.get_id(),
        Some(vec![1]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 2
        },
    );

    // k1 was in old region, but reassigned to new region then.
    cluster.must_put(b"k1", b"v13");
    check_key(&cluster, b"k1", b"v13", Some(true), None, Some(vec![1]));

    // Prepare a peer for FAP.
    pd_client.must_add_peer(new_one_1000_k1.get_id(), new_learner_peer(2, 2003));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        new_one_1000_k1.get_id(),
        Some(vec![1, 2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 2
        },
    );

    // Wait for conf change.
    fail::cfg("fap_ffi_pause", "pause").unwrap();
    fail::cfg("fap_ffi_pause_after_fap_call", "pause").unwrap();
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
    debug!("=== add peer 3001 ===");
    // FAP will ingest data, but not finish applying snapshot due to failpoint.
    pd_client.must_add_peer(new_one_1000_k1.get_id(), new_learner_peer(3, 3001));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        new_one_1000_k1.get_id(),
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 3
        },
    );
    fail::remove("fap_ffi_pause");

    must_wait_until_cond_generic(
        &cluster.cluster_ext,
        SPLIT_REGION_ID,
        Some(vec![3]),
        &|_| {
            let mut persisted = false;
            iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
                persisted = ffi.engine_store_server_helper.query_fap_snapshot_state(
                    SPLIT_REGION_ID,
                    3001,
                    0,
                    0,
                ) == proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted;
            });
            persisted
        },
    );

    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        assert!(!ffi.engine_store_server.kvstore.contains_key(&1000));
    });

    // The snapshot for new_one_1000_k1 is in `tmp_fap_regions`.
    // Raftstore v1 is mono store, so there could be v1 written by old_one_1_k3.
    check_key_ex(
        &cluster,
        b"k1",
        b"v1",
        Some(false),
        None,
        Some(vec![3]),
        Some(new_one_1000_k1.get_id()),
        true,
    );

    // Now the FAP snapshot will stuck at fap_ffi_pause_after_fap_call,
    // We will make the tikv snapshot apply.
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("fap_on_msg_snapshot_1_3003");
    resume_old_tx.send(()).unwrap();

    // A disk key in this mono-store engine does not identify which Region wrote
    // it. Wait for the old parent to replay its split before releasing FAP.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        old_one_1_k3.get_id(),
        Some(vec![3]),
        &|states: &States| states.in_disk_region_state.get_region().get_start_key() == b"k2",
    );
    // Persistence precedes the Raft poller's ApplyResult. Its routing table
    // must also stop covering the child before the synthetic snapshot is sent.
    let store_meta = cluster.store_metas.get(&3).unwrap().clone();
    must_wait_until_cond_generic(&cluster.cluster_ext, 1, Some(vec![3]), &|_| {
        store_meta
            .lock()
            .unwrap()
            .regions
            .get(&1)
            .map_or(false, |region| region.get_start_key() == b"k2")
    });

    check_key_ex(
        &cluster,
        b"k1",
        b"v1",
        None,
        Some(true),
        Some(vec![3]),
        Some(new_one_1000_k1.get_id()),
        true,
    );
    // Make FAP continue after the tikv snapshot is applied.
    fail::cfg("fap_core_must_shared_snapshot", "panic").unwrap();
    fail::remove("fap_ffi_pause_after_fap_call");
    debug!("=== peer 3001 allow send snap ===");
    check_key_ex(
        &cluster,
        b"k1",
        b"v13",
        None,
        Some(true),
        Some(vec![3]),
        Some(new_one_1000_k1.get_id()),
        true,
    );

    fail::remove("fap_core_fallback_millis");
    fail::remove("fap_core_must_shared_snapshot");
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("on_can_apply_snapshot");
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}

fn do_test_stale_split_child(tombstone: bool) {
    let (mut cluster, _) = new_mock_cluster_snap(0, 1);
    disable_auto_gen_compact_log(&mut cluster);
    cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let parent_meta = cluster.get_region(b"k1");

    iter_ffi_helpers(&cluster, Some(vec![1]), &mut |_, ffi: &mut FFIHelperSet| {
        let store = &mut ffi.engine_store_server;
        let parent = store.kvstore.get_mut(&1).unwrap();
        // Bootstrap writes create a mock Region before an admin response has
        // supplied its metadata. Seed it from Proxy for this direct FFI replay.
        parent.region = parent_meta.clone();
        parent.peer = parent_meta
            .get_peers()
            .iter()
            .find(|peer| peer.get_store_id() == 1)
            .unwrap()
            .clone();
        let index = parent.apply_state.get_applied_index() + 1;
        let term = parent.applied_term;
        let mut derived = parent.region.clone();
        derived.set_start_key(b"k2".to_vec());
        let mut split_child = parent.region.clone();
        split_child.set_id(1000);
        split_child.set_end_key(b"k2".to_vec());
        split_child.set_peers(vec![test_raftstore::new_peer(1, 1002)].into());

        // Old writes and deletes must not overwrite data owned by a newer peer
        // in the shared engine, even in column families other than default.
        for cf in 0..3 {
            parent.pending_write[cf].insert(b"k1".to_vec(), b"v1".to_vec());
            parent.pending_delete[cf].insert(b"k0".to_vec());
        }
        let mut current_child = split_child.clone();
        current_child.set_peers(vec![test_raftstore::new_peer(1, 3001)].into());
        let mut child_state = RegionLocalState::default();
        child_state.set_region(current_child.clone());
        child_state.set_state(if tombstone {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        });
        let engine = &store.engines.as_ref().unwrap().kv.rocks;
        engine
            .put_msg_cf(CF_RAFT, &keys::region_state_key(1000), &child_state)
            .unwrap();
        for cf in [CF_DEFAULT, CF_LOCK, CF_WRITE] {
            engine.put_cf(cf, &keys::data_key(b"k1"), b"v13").unwrap();
            engine
                .put_cf(cf, &keys::data_key(b"k0"), b"v0-new")
                .unwrap();
        }
        if !tombstone {
            let mut child = make_new_region(Some(current_child), Some(1));
            child.set_applied(10, 6);
            for cf in 0..3 {
                write_kv_in_mem(&mut child, cf, b"k1", b"v13");
                write_kv_in_mem(&mut child, cf, b"k0", b"v0-new");
            }
            store.kvstore.insert(1000, Box::new(child));
        }

        let mut request = kvproto::raft_cmdpb::AdminRequest::default();
        request.set_cmd_type(AdminCmdType::BatchSplit);
        let mut response = kvproto::raft_cmdpb::AdminResponse::default();
        response.mut_splits().mut_regions().push(split_child);
        response.mut_splits().mut_regions().push(derived);
        ffi.engine_store_server_helper.handle_admin_raft_cmd(
            &request,
            &response,
            proxy_ffi::interfaces_ffi::RaftCmdHeader::new(1, index, term),
        );

        let store = &ffi.engine_store_server;
        if tombstone {
            assert!(!store.kvstore.contains_key(&1000));
        } else {
            let child = store.kvstore.get(&1000).unwrap();
            assert_eq!(child.peer.get_id(), 3001);
            assert_eq!(child.apply_state.get_applied_index(), 10);
            for cf in 0..3 {
                assert_eq!(child.data[cf].get(b"k1".as_slice()).unwrap(), b"v13");
                assert_eq!(child.data[cf].get(b"k0".as_slice()).unwrap(), b"v0-new");
            }
        }
        let parent = store.kvstore.get(&1).unwrap();
        let default_cf = proxy_ffi::interfaces_ffi::ColumnFamilyType::Default as usize;
        assert_eq!(
            parent.data[default_cf].get(b"k3".as_slice()).unwrap(),
            b"v3"
        );
        assert!(!parent.data[default_cf].contains_key(b"k1".as_slice()));
        let engine = &store.engines.as_ref().unwrap().kv.rocks;
        for cf in [CF_DEFAULT, CF_LOCK, CF_WRITE] {
            assert_eq!(
                engine
                    .get_value_cf(cf, &keys::data_key(b"k1"))
                    .unwrap()
                    .unwrap(),
                b"v13"
            );
            assert_eq!(
                engine
                    .get_value_cf(cf, &keys::data_key(b"k0"))
                    .unwrap()
                    .unwrap(),
                b"v0-new"
            );
        }
    });
    cluster.shutdown();
}

#[test]
fn test_stale_split_does_not_revive_tombstone_child() {
    do_test_stale_split_child(true);
}

#[test]
fn test_stale_split_preserves_newer_child() {
    do_test_stale_split_child(false);
}

// If the peer is initialized, it will not use fap to catch up.
#[test]
fn test_existing_peer() {
    // Can always apply snapshot immediately
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();

    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();
    must_put_and_check_key(&mut cluster, 1, 2, Some(true), None, Some(vec![1]));

    fail::cfg("fap_core_no_fallback", "panic").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    must_put_and_check_key(&mut cluster, 3, 4, Some(true), None, None);
    fail::remove("fap_core_no_fallback");

    stop_tiflash_node(&mut cluster, 2);

    cluster.must_put(b"k5", b"v5");
    cluster.must_put(b"k6", b"v6");
    force_compact_log(&mut cluster, b"k6", Some(vec![1]));

    fail::cfg("fap_core_no_fast_path", "panic").unwrap();

    restart_tiflash_node(&mut cluster, 2);

    // No tikv snapshot before.
    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            assert_eq!(e.apply_snap_count.load(Ordering::SeqCst), 0);
        });
    });

    check_key(&mut cluster, b"k6", b"v6", Some(true), None, None);

    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            assert_eq!(e.apply_snap_count.load(Ordering::SeqCst), 1);
        });
    });

    cluster.shutdown();
    fail::remove("fap_core_no_fast_path");
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
}

// We will reject remote peer in Applying state.
#[test]
fn test_apply_snapshot() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    must_put_and_check_key(&mut cluster, 1, 2, Some(true), None, Some(vec![1]));

    // We add peer 3 from peer 2, it will be paused before fetching peer 2's data.
    // However, peer 2 will apply conf change.
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
    fail::cfg("fap_ffi_pause", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    must_put_and_check_key(&mut cluster, 2, 3, Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
        },
    );

    // Peer 2 can't apply new kvs.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Both),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgSnapshot)
            .direction(Direction::Both),
    ));
    cluster.must_put(b"k3", b"v3");
    cluster.must_put(b"k4", b"v4");
    cluster.must_put(b"k5", b"v5");
    // Log compacted, peer 2 will get snapshot, however, we pause when applying
    // snapshot.
    force_compact_log(&mut cluster, b"k2", Some(vec![1]));
    // Wait log compacted.
    std::thread::sleep(std::time::Duration::from_millis(1000));
    fail::cfg("on_ob_post_apply_snapshot", "pause").unwrap();
    // Trigger a snapshot to 2.
    cluster.clear_send_filters();

    debug!("wait applying snapshot of peer 2");
    // Wait until peer 2 in Applying state.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_state() == PeerState::Applying
        },
    );

    // Now if we continue fast path, peer 2 will be in Applying state.
    // Peer 3 can't use peer 2's data.
    // We will end up going slow path.
    fail::remove("fap_ffi_pause");
    fail::cfg("fap_core_no_fast_path", "panic").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    // Resume applying snapshot
    fail::remove("on_ob_post_apply_snapshot");
    check_key(&cluster, b"k4", b"v4", Some(true), None, Some(vec![1, 3]));
    cluster.shutdown();
    fail::remove("fap_core_no_fast_path");
    fail::remove("fap_mock_add_peer_from_id");
    // fail::remove("before_tiflash_check_double_write");
}

#[test]
fn test_split_no_fast_add() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    let _ = cluster.run();

    // Compose split keys
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k1", b"v1", Some(true), None, None);
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);
    let r1 = cluster.get_region(b"k1");
    let r3 = cluster.get_region(b"k3");
    assert_eq!(r1.get_id(), r3.get_id());

    fail::cfg("fap_core_no_fast_path", "panic").unwrap();
    cluster.must_split(&r1, b"k2");
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1000,
        None,
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 3
        },
    );
    let _r1_new = cluster.get_region(b"k1"); // 1000
    let _r3_new = cluster.get_region(b"k3"); // 1
    cluster.must_put(b"k0", b"v0");
    check_key(&cluster, b"k0", b"v0", Some(true), None, None);

    fail::remove("fap_core_no_fast_path");
    fail::remove("on_can_apply_snapshot");
    cluster.shutdown();
}

#[test]
fn test_split_merge() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    let _ = cluster.run_conf_change();

    // Compose split keys
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));
    let r1 = cluster.get_region(b"k1");
    let r3 = cluster.get_region(b"k3");
    assert_eq!(r1.get_id(), r3.get_id());

    cluster.must_split(&r1, b"k2");
    let r1_new = cluster.get_region(b"k1"); // 1000
    let r3_new = cluster.get_region(b"k3"); // 1
    let r1_id = r1_new.get_id();
    let r3_id = r3_new.get_id();
    debug!("r1_new {} r3_new {}", r1_id, r3_id);

    // Test add peer after split
    pd_client.must_add_peer(r1_id, new_learner_peer(2, 2001));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![2]));
    check_key(&cluster, b"k3", b"v3", Some(false), None, Some(vec![2]));
    pd_client.must_add_peer(r3_id, new_learner_peer(2, 2003));
    std::thread::sleep(std::time::Duration::from_millis(1000));
    check_key(&cluster, b"k1", b"v1", Some(false), None, Some(vec![2]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![2]));

    // Test merge
    pd_client.must_add_peer(r3_id, new_learner_peer(3, 3003));
    pd_client.merge_region(r1_id, r3_id);
    must_not_merged(pd_client.clone(), r1_id, Duration::from_millis(1000));
    pd_client.must_add_peer(r1_id, new_learner_peer(3, 3001));
    pd_client.must_merge(r1_id, r3_id);
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![3]));
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![3]));

    fail::remove("on_can_apply_snapshot");
    cluster.shutdown();
}

// Fallback to slow path in if fast_add_peer call fails.
// If the snapshot could be reused, so we can see apply fap snapshot is
// successful. Otherwise, it will prehandle again.
fn do_test_fall_back_to_slow_path(reuse_fap_snapshot: bool) {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    if reuse_fap_snapshot {
        fail::cfg("fap_mock_fail_after_write", "return(1)").unwrap();
        fail::cfg("fap_core_must_shared_snapshot", "panic").unwrap();
    } else {
        fail::cfg("fap_mock_fail_after_write_mismatch", "return(1)").unwrap();
        fail::cfg("fap_core_must_not_shared_snapshot", "panic").unwrap();
    }
    fail::cfg("fap_core_no_fast_path", "panic").unwrap();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    // FAP will fail for "can't find entry for index 9 of region 1".
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    must_wait_fap_snapshot_removed(&cluster.cluster_ext, 1, 2, 2);

    fail::remove("fap_mock_fail_after_write");
    fail::remove("on_can_apply_snapshot");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_core_no_fast_path");
    cluster.shutdown();
}

#[test]
fn test_fall_back_to_slow_path_1() {
    do_test_fall_back_to_slow_path(true);
}

#[test]
fn test_fall_back_to_slow_path_0() {
    do_test_fall_back_to_slow_path(false);
}

#[test]
fn test_single_replica_migrate() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    // Fast add peer 2
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();

    // Remove peer 2.
    pd_client.must_remove_peer(1, new_learner_peer(2, 2));
    must_wait_until_cond_generic(&cluster.cluster_ext, 1, None, &|states: &HashMap<
        u64,
        States,
    >|
     -> bool {
        states.get(&2).is_none()
    });

    // Remove peer 2 and then add some new logs.
    cluster.must_put(b"krm2", b"v");
    check_key(&cluster, b"krm2", b"v", Some(true), None, Some(vec![1]));

    // Try fast add peer from removed peer 2.
    // TODO It will fallback to slow path if we don't support single replica
    // migration.
    fail::cfg("fap_core_no_fast_path", "panic").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    check_key(&cluster, b"krm2", b"v", Some(true), None, Some(vec![3]));
    std::thread::sleep(std::time::Duration::from_millis(2000));
    must_wait_until_cond_generic(&cluster.cluster_ext, 1, None, &|states: &HashMap<
        u64,
        States,
    >|
     -> bool {
        states.get(&3).is_some()
    });
    fail::remove("fap_core_no_fast_path");

    fail::remove("on_can_apply_snapshot");
    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}

// Test MsgSnapshot before MsgAppend
/// According to https://github.com/tikv/raft-rs/blob/2aefbf627f243dd261b7585ef1250d32efd9dfe7/src/raft.rs#L842,
/// if log is truncated in Leader, a MsgSnapshot may be sent directly before a
/// MsgAppend. If such MsgSnapshot is received when a FAP snapshot IS BUILDING,
/// then it will be dropped.
#[test]
fn test_msgsnapshot_before_msgappend() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    fail::cfg("fap_core_no_fallback", "panic").unwrap();
    fail::cfg("fap_mock_force_wait_for_data", "return(1)").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));

    std::thread::sleep(Duration::from_secs(1));

    // Trigger direct MsgSnapshot.
    let region = cluster.get_region("k1".as_bytes());
    let prev_state = maybe_collect_states(&cluster.cluster_ext, 1, Some(vec![1]));
    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    debug!("compact at index {}", compact_index);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(1, region.get_region_epoch(), compact_log);
    let _res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    let mut t = 0;
    loop {
        let mut buf = Vec::<raft::eraftpb::Entry>::new();
        cluster
            .get_engines(1)
            .raft
            .get_all_entries_to(1, &mut buf)
            .unwrap();
        if buf.len() == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        t += 1;
        assert!(t < 11);
    }

    // MsgSnapshot will be rejected before.
    fail::remove("fap_mock_force_wait_for_data");
    cluster.clear_send_filters();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));

    std::thread::sleep(std::time::Duration::from_secs(1));

    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        let mut x: u64 = 0;
        let mut y: u64 = 0;
        (*ffi.engine_store_server).mutate_region_states_mut(1, |e: &mut RegionStats| {
            x = e.finished_fast_add_peer_count.load(Ordering::SeqCst);
        });
        (*ffi.engine_store_server).mutate_region_states_mut(1, |e: &mut RegionStats| {
            y = e.started_fast_add_peers.lock().unwrap().len() as u64;
        });
        assert_eq!(x, y);
    });

    // FAP will fail for "can't find entry for index 9 of region 1".
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    must_wait_fap_snapshot_removed(&cluster.cluster_ext, 1, 2, 2);

    fail::remove("on_can_apply_snapshot");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_core_no_fallback");
    cluster.shutdown();
}

#[test]
fn test_replay_fap_after_post_apply() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    // If we return prematurely by `region_apply_snap`, `status` maybe switched to
    // OK, then raft logs will be handled, thus leading to unintentianlly region
    // ingesting due to mock-store. Disable compact log can prevent this,
    // because in this test, only compact log could be generated.
    cluster.mut_config().raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.mut_config().raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100000);
    cluster.mut_config().raft_store.raft_log_gc_threshold = 10000;

    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    fail::cfg("fap_core_no_fallback", "panic").unwrap();
    fail::cfg("region_apply_snap", "return").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));

    check_key(&cluster, b"k2", b"v2", Some(false), None, Some(vec![2]));

    must_wait_fap_snapshot_applying(&cluster.cluster_ext, 1, 2, 2);
    stop_tiflash_node(&mut cluster, 2);
    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 2, 0, 0),
            proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
        );
    });
    fail::remove("region_apply_snap");
    restart_tiflash_node(&mut cluster, 2);
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![2]));

    // After the snapshto data is written, the fap snapshot is deleted.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_state() != PeerState::Applying
        },
    );

    must_wait_fap_snapshot_removed(&cluster.cluster_ext, 1, 2, 2);
    fail::remove("on_pre_write_apply_state");
    fail::remove("on_can_apply_snapshot");
    fail::remove("post_apply_snapshot_allow_no_unips");
    cluster.shutdown();
}

#[test]
fn test_replay_fap_before_post_apply() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    cluster.mut_config().raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.mut_config().raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100000);
    cluster.mut_config().raft_store.raft_log_gc_threshold = 10000;
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    fail::cfg("fap_core_no_fallback", "panic").unwrap();
    fail::cfg("on_ob_post_apply_snapshot", "return").unwrap();
    fail::cfg("region_apply_snap", "return").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));

    check_key(&cluster, b"k2", b"v2", Some(false), None, Some(vec![2]));

    must_wait_fap_snapshot_applying(&cluster.cluster_ext, 1, 2, 2);
    stop_tiflash_node(&mut cluster, 2);
    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 2, 0, 0),
            proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
        );
    });

    fail::remove("on_ob_post_apply_snapshot");
    fail::remove("region_apply_snap");
    restart_tiflash_node(&mut cluster, 2);
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![2]));

    // After the snapshot data is written, the fap snapshot is deleted.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_state() != PeerState::Applying
        },
    );

    must_wait_fap_snapshot_removed(&cluster.cluster_ext, 1, 2, 2);
    fail::remove("on_pre_write_apply_state");
    fail::remove("on_can_apply_snapshot");
    fail::remove("post_apply_snapshot_allow_no_unips");
    cluster.shutdown();
}

// TODO This test can work if we support `return` in
// region_apply_return_not_change_state. #[test]
fn test_replay_fap_before_write() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    pd_client.disable_default_operator();
    setup_fp_for_fap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    fail::cfg("fap_core_no_fallback", "panic").unwrap();
    fail::cfg("region_apply_snap_before_write", "return").unwrap();
    fail::cfg("region_apply_return_not_change_state", "return").unwrap();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));

    check_key(&cluster, b"k2", b"v2", Some(false), None, Some(vec![2]));

    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_state() == PeerState::Applying
        },
    );

    cluster.must_put(b"k3", b"v2");
    check_key(&cluster, b"k3", b"v2", Some(false), None, Some(vec![2]));

    std::thread::sleep(std::time::Duration::from_secs(1));

    stop_tiflash_node(&mut cluster, 2);
    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 2, 0, 0),
            proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
        );
    });

    fail::remove("region_apply_snap_before_write");
    fail::remove("region_apply_return_not_change_state");
    restart_tiflash_node(&mut cluster, 2);
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![2]));

    // After the snapshto data is written, the fap snapshot is deleted.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            states.in_disk_region_state.get_state() != PeerState::Applying
        },
    );

    iter_ffi_helpers(&cluster, Some(vec![2]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 2, 0, 0),
            proxy_ffi::interfaces_ffi::FapSnapshotState::NotFound
        );
    });

    check_key(&cluster, b"k3", b"v2", Some(true), None, Some(vec![2]));

    fail::remove("on_pre_write_apply_state");
    fail::remove("on_can_apply_snapshot");
    fail::remove("post_apply_snapshot_allow_no_unips");
    cluster.shutdown();
}
