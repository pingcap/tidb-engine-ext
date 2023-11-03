// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::utils::v1::*;

#[derive(PartialEq, Eq)]
enum SourceType {
    Leader,
    Learner,
    // The learner coesn't catch up with Leader.
    DelayedLearner,
    InvalidSource,
}

#[derive(PartialEq, Eq, Debug)]
enum PauseType {
    None,
    Build,
    ApplySnapshot,
    SendFakeSnapshot,
}

// This test is covered in `simple_fast_add_peer`.
// It is here only as a demo for easy understanding the whole process.
// #[test]
fn basic_fast_add_peer() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    fail::cfg("fap_mock_fake_snapshot", "return(1)").unwrap();
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    cluster.shutdown();
    fail::remove("fap_core_no_fallback");
    fail::remove("fap_mock_fake_snapshot");
    // fail::remove("before_tiflash_check_double_write");
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

    // Delay, so the legacy snapshot comes after fap snapshot in pending_applies
    // queue.
    fail::cfg("on_ob_pre_handle_snapshot_s3", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3003));
    std::thread::sleep(std::time::Duration::from_millis(1000));

    // Split
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));
    // Generates 2 peers {1001@1, 1002@3} for region 1.
    // However, we use the older snapshot, so the 1002 peer is not inited.
    cluster.must_split(&cluster.get_region(b"k1"), b"k2");

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

// If a legacy snapshot is applied between fn_fast_add_peer and
// build_and_send_snapshot, it will override the previous snapshot's data, which
// is actually newer.

#[test]
fn test_overlap_apply_legacy_in_the_middle() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
    disable_auto_gen_compact_log(&mut cluster);
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

    // Use an invalid store id to make FAP fallback.
    fail::cfg("fap_mock_add_peer_from_id", "return(4)").unwrap();

    // Don't use send filter to prevent applying snapshot,
    // since it may no longer send snapshot after split.
    fail::cfg("fap_on_msg_snapshot_1_3003", "pause").unwrap();
    pd_client.must_add_peer(1, new_learner_peer(3, 3003));
    std::thread::sleep(std::time::Duration::from_millis(1000));

    // Split
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1]));
    // Generates 2 peers {1001@1, 1002@3} for region 1.
    // However, we use the older snapshot, so the 1002 peer is not inited.
    cluster.must_split(&cluster.get_region(b"k1"), b"k2");

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

    // Wait for conf change.
    fail::cfg("fap_ffi_pause", "pause").unwrap();
    fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
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
    std::thread::sleep(std::time::Duration::from_millis(1000));
    fail::cfg("fap_ffi_pause_after_fap_call", "pause").unwrap();
    fail::remove("fap_ffi_pause");

    // std::thread::sleep(std::time::Duration::from_millis(5000));
    check_key_ex(
        &cluster,
        b"k1",
        b"v13",
        None,
        Some(true),
        Some(vec![3]),
        None,
        true,
    );

    // Now the FAP snapshot will stuck at fap_ffi_pause_after_fap_call,
    // We will make the legacy one apply.
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("fap_on_msg_snapshot_1_3003");

    // std::thread::sleep(std::time::Duration::from_millis(5000));
    check_key_ex(
        &cluster,
        b"k1",
        b"v1",
        None,
        Some(true),
        Some(vec![3]),
        None,
        true,
    );
    // Make FAP continue after the legacy snapshot is applied.
    fail::remove("fap_ffi_pause_after_fap_call");
    // TODO wait until fap finishes.
    // std::thread::sleep(std::time::Duration::from_millis(5000));
    check_key_ex(
        &cluster,
        b"k1",
        b"v1",
        None,
        Some(true),
        Some(vec![3]),
        None,
        true,
    );

    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("on_can_apply_snapshot");
    fail::remove("apply_on_handle_snapshot_sync");
    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}

// `block_wait`: whether we block wait in a MsgAppend handling, or return with
// WaitForData. `pause`: pause in some core procedures.
// `check_timeout`: mock and check if FAP timeouts.
fn simple_fast_add_peer(
    source_type: SourceType,
    block_wait: bool,
    pause: PauseType,
    check_timeout: bool,
) {
    // The case in TiFlash is (DelayedPeer, false, Build)
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    if !check_timeout {
        fail::cfg("fap_core_fallback_millis", "return(1000000)").unwrap();
    } else {
        fail::cfg("fap_core_fallback_millis", "return(1500)").unwrap();
    }
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    if block_wait {
        fail::cfg("fap_mock_block_wait", "return(1)").unwrap();
    }
    match pause {
        PauseType::ApplySnapshot => {
            cluster.cfg.tikv.raft_store.region_worker_tick_interval = ReadableDuration::millis(500);
        }
        _ => (),
    }
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    // If we don't write here, we will have the first MsgAppend with (6,6), which
    // will cause "fast-forwarded commit to snapshot".
    cluster.must_put(b"k0", b"v0");

    // Add learner 2 from leader 1
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    // std::thread::sleep(std::time::Duration::from_millis(2000));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    // Getting (k1,v1) not necessarily means peer 2 is ready.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    // Add learner 3 according to source_type
    match source_type {
        SourceType::Learner | SourceType::DelayedLearner => {
            fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
        }
        SourceType::InvalidSource => {
            fail::cfg("fap_mock_add_peer_from_id", "return(100)").unwrap();
        }
        _ => (),
    };

    match pause {
        PauseType::Build => fail::cfg("fap_ffi_pause", "pause").unwrap(),
        PauseType::ApplySnapshot => {
            assert!(
                cluster
                    .cfg
                    .proxy_cfg
                    .raft_store
                    .region_worker_tick_interval
                    .as_millis()
                    < 1000
            );
            assert!(
                cluster
                    .cfg
                    .tikv
                    .raft_store
                    .region_worker_tick_interval
                    .as_millis()
                    < 1000
            );
            fail::cfg("on_can_apply_snapshot", "return(false)").unwrap()
        }
        PauseType::SendFakeSnapshot => {
            fail::cfg("fap_core_fake_send", "return(1)").unwrap();
            // If we fake send snapshot, then fast path will certainly fail.
            // Then we will timeout in FALLBACK_MILLIS and go to slow path.
        }
        _ => (),
    }

    // Add peer 3 by FAP
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    cluster.must_put(b"k2", b"v2");

    let need_fallback = check_timeout;

    // If we need to fallback to slow path,
    // we must make sure the data is persisted before Leader generated snapshot.
    // This is necessary, since we haven't adapt `handle_snapshot`,
    // which is a leader logic.
    if need_fallback {
        assert!(pause == PauseType::SendFakeSnapshot);
        check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1]));
        iter_ffi_helpers(
            &cluster,
            Some(vec![1]),
            &mut |_, ffi: &mut FFIHelperSet| unsafe {
                let server = ffi.engine_store_server.as_mut();
                server.write_to_db_by_region_id(1, "persist for up-to-date snapshot".to_string());
            },
        );
    }

    match source_type {
        SourceType::DelayedLearner => {
            // Make sure conf change is applied in peer 2.
            check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
            cluster.add_send_filter(CloneFilterFactory(
                RegionPacketFilter::new(1, 2)
                    .msg_type(MessageType::MsgAppend)
                    .msg_type(MessageType::MsgSnapshot)
                    .direction(Direction::Recv),
            ));
            cluster.must_put(b"k3", b"v3");
        }
        _ => (),
    };

    // Wait some time and then recover.
    match pause {
        PauseType::Build => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            fail::remove("fap_ffi_pause");
        }
        PauseType::ApplySnapshot => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            check_key(&cluster, b"k2", b"v2", Some(false), None, Some(vec![3]));
            fail::remove("on_can_apply_snapshot");
            fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
            // Wait tick for region worker.
            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
        PauseType::SendFakeSnapshot => {
            // Wait FALLBACK_MILLIS
            std::thread::sleep(std::time::Duration::from_millis(3000));
            fail::remove("fap_core_fake_send");
            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
        _ => (),
    }

    // Check stage 1.
    match source_type {
        SourceType::DelayedLearner => {
            check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1, 3]));
            check_key(&cluster, b"k3", b"v3", Some(false), None, Some(vec![2]));
        }
        SourceType::Learner => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
        _ => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
    };
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![3]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
        },
    );

    match pause {
        PauseType::ApplySnapshot => {
            iter_ffi_helpers(
                &cluster,
                Some(vec![3]),
                &mut |_, _ffi: &mut FFIHelperSet| {
                    // Not actually the case, since we allow handling
                    // MsgAppend multiple times.
                    // So the following fires when:
                    // (DelayedLearner, false, ApplySnapshot)

                    // let server = &ffi.engine_store_server;
                    // (*ffi.engine_store_server).mutate_region_states(1, |e:
                    // &mut RegionStats| { assert_eq!(1,
                    // e.fast_add_peer_count.load(Ordering::SeqCst));
                    // });
                },
            );
        }
        _ => (),
    }

    match source_type {
        SourceType::DelayedLearner => {
            cluster.clear_send_filters();
        }
        _ => (),
    };

    // Destroy peer, and then try re-add a new peer of the same region.
    pd_client.must_remove_peer(1, new_learner_peer(3, 3));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![1]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_none()
        },
    );
    std::thread::sleep(std::time::Duration::from_millis(1000));
    // Assert the peer removing succeeed.
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        let server = &ffi.engine_store_server;
        assert!(!server.kvstore.contains_key(&1));
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            e.fast_add_peer_count.store(0, Ordering::SeqCst);
        });
    });
    cluster.must_put(b"k5", b"v5");
    // These failpoints make sure we will cause again a fast path.
    if source_type == SourceType::InvalidSource {
        // If we still use InvalidSource, we still need to goto slow path.
    } else {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
    }
    // Re-add peer in store.
    pd_client.must_add_peer(1, new_learner_peer(3, 4));
    // Wait until Learner has applied ConfChange
    std::thread::sleep(std::time::Duration::from_millis(1000));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![3]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 4).is_some()
        },
    );
    // If we re-add peer, we can still go fast path.
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            assert!(e.fast_add_peer_count.load(Ordering::SeqCst) > 0);
        });
    });
    cluster.must_put(b"k6", b"v6");
    check_key(
        &cluster,
        b"k6",
        b"v6",
        Some(true),
        None,
        Some(vec![1, 2, 3]),
    );
    fail::remove("fap_core_no_fallback");
    fail::remove("fast_path_is_not_first");

    fail::remove("on_can_apply_snapshot");
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_core_fallback_millis");
    fail::remove("fap_mock_block_wait");
    cluster.shutdown();
}

mod simple_normal {
    use super::*;
    #[test]
    fn test_simple_from_leader() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Leader, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// Fast path by learner snapshot.
    #[test]
    fn test_simple_from_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// If a learner is delayed, but already applied ConfChange.
    #[test]
    fn test_simple_from_delayed_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// If we select a wrong source, or we can't run fast path, we can fallback
    /// to normal.
    #[test]
    fn test_simple_from_invalid_source() {
        simple_fast_add_peer(SourceType::InvalidSource, false, PauseType::None, false);
    }
}

mod simple_blocked_nopause {}

mod simple_blocked_pause {
    use super::*;
    // Delay when fetch and build data

    #[test]
    fn test_simpleb_from_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        // Need to changed to pre_write_apply_state
        fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
        simple_fast_add_peer(SourceType::Learner, true, PauseType::Build, false);
        fail::remove("on_pre_write_apply_state");
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simpleb_from_delayed_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        // Need to changed to pre_write_apply_state
        fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, true, PauseType::Build, false);
        fail::remove("on_pre_write_apply_state");
        fail::remove("fap_core_no_fallback");
    }

    // Delay when applying snapshot
    // This test is origially aimed to test multiple MsgSnapshot.
    // However, we observed less repeated MsgAppend than in real cluster.
    #[test]
    fn test_simpleb_from_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, true, PauseType::ApplySnapshot, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simpleb_from_delayed_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(
            SourceType::DelayedLearner,
            true,
            PauseType::ApplySnapshot,
            false,
        );
        fail::remove("fap_core_no_fallback");
    }
}

mod simple_non_blocked_non_pause {
    use super::*;
    #[test]
    fn test_simplenb_from_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simplenb_from_delayed_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }
}

mod simple_non_blocked_pause {
    use super::*;
    #[test]
    fn test_simplenb_from_delayed_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::Build, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simplenb_from_delayed_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(
            SourceType::DelayedLearner,
            false,
            PauseType::ApplySnapshot,
            false,
        );
        fail::remove("fap_core_no_fallback");
    }
}

#[test]
fn test_timeout_fallback() {
    fail::cfg("on_pre_write_apply_state", "return").unwrap();
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // By sending SendFakeSnapshot we can observe timeout.
    simple_fast_add_peer(
        SourceType::Learner,
        false,
        PauseType::SendFakeSnapshot,
        true,
    );
    fail::remove("on_pre_write_apply_state");
    fail::remove("apply_on_handle_snapshot_sync");
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

    // peer 2 can't apply new kvs.
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

#[test]
fn test_fall_back_to_slow_path() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 2);
    pd_client.disable_default_operator();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;

    tikv_util::set_panic_hook(true, "./");
    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    let _ = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));
    cluster.must_put(b"k2", b"v2");

    fail::cfg("fap_mock_fail_after_write", "return(1)").unwrap();
    fail::cfg("fap_core_no_fast_path", "panic").unwrap();

    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    fail::remove("fap_mock_fail_after_write");
    fail::remove("on_can_apply_snapshot");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_core_no_fast_path");
    cluster.shutdown();
}

#[test]
fn test_single_replica_migrate() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    pd_client.disable_default_operator();
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
