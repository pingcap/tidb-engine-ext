// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::proxy::*;

#[test]
fn test_interaction() {
    // TODO Maybe we should pick this test to TiKV.
    // This test is to check if empty entries can affect pre_exec and post_exec.
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    fail::cfg("try_flush_data", "return(0)").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();

    // Wait until all nodes have (k1, v1).
    check_key(&cluster, b"k1", b"v1", Some(true), None, None);

    let prev_states = collect_all_states(&cluster, region_id);
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let _ = cluster
        .call_command_on_leader(req.clone(), Duration::from_secs(3))
        .unwrap();

    // Empty result can also be handled by post_exec
    let new_states = must_wait_until_cond_states(
        &cluster,
        region_id,
        &prev_states,
        &|old: &States, new: &States| {
            // Must wait advance of apply_index.
            old.in_memory_apply_state != new.in_memory_apply_state
                || old.in_memory_applied_term != new.in_memory_applied_term
        },
    );

    compare_states(&prev_states, &new_states, &|old: &States, new: &States| {
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        // An empty cmd will not cause persistence.
        assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
    });

    cluster.must_put(b"k2", b"v2");
    // Wait until all nodes have (k2, v2).
    check_key(&cluster, b"k2", b"v2", Some(true), None, None);

    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    let prev_states = collect_all_states(&cluster, region_id);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(400));
    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
    }

    fail::remove("try_flush_data");
    fail::remove("on_empty_cmd_normal");
    cluster.shutdown();
}

#[test]
fn test_leadership_change_filter() {
    test_leadership_change_impl(true);
}

#[test]
fn test_leadership_change_no_persist() {
    test_leadership_change_impl(false);
}

fn test_leadership_change_impl(filter: bool) {
    // Test if a empty command can be observed when leadership changes.
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    disable_auto_gen_compact_log(&mut cluster);

    if filter {
        // We don't handle CompactLog at all.
        fail::cfg("try_flush_data", "return(0)").unwrap();
    } else {
        // We don't return Persist after handling CompactLog.
        fail::cfg("no_persist_compact_log", "return").unwrap();
    }
    // Do not handle empty cmd.
    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();

    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());

    cluster.must_put(b"k2", b"v2");
    fail::cfg("on_empty_cmd_normal", "return").unwrap();

    // Wait until all nodes have (k2, v2), then transfer leader.
    check_key(&cluster, b"k2", b"v2", Some(true), None, None);
    if filter {
        // We should also filter normal kv, since a empty result can also be invoke
        // pose_exec.
        fail::cfg("on_post_exec_normal", "return(false)").unwrap();
    }
    let prev_states = collect_all_states(&cluster, region_id);
    cluster.must_transfer_leader(region.get_id(), peer_2.clone());

    // The states remain the same, since we don't observe empty cmd.
    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        if filter {
            // CompactLog can still change in-memory state, when exec in memory.
            assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        }
        assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
    }

    fail::remove("on_empty_cmd_normal");
    // We need forward empty cmd generated by leadership changing to TiFlash.
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());
    std::thread::sleep(std::time::Duration::from_secs(1));

    let new_states = collect_all_states(&cluster, region_id);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_altered_memory_apply_term(&prev_states, &new_states);

    if filter {
        fail::remove("try_flush_data");
        fail::remove("on_post_exec_normal");
    } else {
        fail::remove("no_persist_compact_log");
    }
    cluster.shutdown();
}

#[test]
fn test_kv_write_always_persist() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    let _ = cluster.run();

    cluster.must_put(b"k0", b"v0");
    let region_id = cluster.get_region(b"k0").get_id();

    let mut prev_states = collect_all_states(&cluster, region_id);
    // Always persist on every command
    fail::cfg("on_post_exec_normal_end", "return(true)").unwrap();
    for i in 1..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());

        // We can't always get kv from disk, even we commit everytime,
        // since they are filtered by engint_tiflash
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);

        // This may happen after memory write data and before commit.
        // We must check if we already have in memory.
        check_apply_state(&cluster, region_id, &prev_states, Some(false), None);
        std::thread::sleep(std::time::Duration::from_millis(20));
        // However, advanced apply index will always persisted.
        let new_states = collect_all_states(&cluster, region_id);
        must_altered_disk_apply_state(&prev_states, &new_states);
        prev_states = new_states;
    }
    fail::remove("on_post_exec_normal_end");
    cluster.shutdown();
}

#[test]
fn test_kv_write() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    fail::cfg("on_post_exec_normal", "return(false)").unwrap();
    fail::cfg("on_post_exec_admin", "return(false)").unwrap();
    // Abandon CompactLog and previous flush.
    fail::cfg("try_flush_data", "return(0)").unwrap();

    let _ = cluster.run();

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we disable all observers, we can get nothing in either memory and disk.
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(false),
            Some(false),
            None,
        );
    }

    // We can read initial raft state, since we don't persist meta either.
    let r1 = cluster.get_region(b"k1").get_id();
    let prev_states = collect_all_states(&cluster, r1);

    fail::remove("on_post_exec_normal");
    fail::remove("on_post_exec_admin");
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we enable all observers, we can get in memory.
    // However, we get nothing in disk since we don't persist.
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(false),
            None,
        );
    }

    let new_states = collect_all_states(&cluster, r1);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_unaltered_disk_apply_state(&prev_states, &new_states);

    std::thread::sleep(std::time::Duration::from_millis(20));
    fail::remove("try_flush_data");

    let prev_states = collect_all_states(&cluster, r1);
    // Write more after we force persist when CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // We can read from mock-store's memory, we are not sure if we can read from
    // disk, since there may be or may not be a CompactLog.
    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    // Force a compact log to persist.
    let region_r = cluster.get_region("k1".as_bytes());
    let region_id = region_r.get_id();
    let compact_log = test_raftstore::new_compact_log_request(1000, 100);
    let req =
        test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);
    // This CompactLog is executed with an error. It will not trigger a compaction.
    // However, it can trigger a persistence.
    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(true),
            None,
        );
    }

    let new_states = collect_all_states(&cluster, r1);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_altered_disk_apply_state(&prev_states, &new_states);

    fail::remove("no_persist_compact_log");
    cluster.shutdown();
}

#[test]
fn test_unsupport_admin_cmd() {
    // ComputeHash and VerifyHash shall be filtered.
    let (mut cluster, _pd_client) = new_mock_cluster(0, 2);

    cluster.run();

    cluster.must_put(b"k", b"v");
    let region = cluster.get_region("k".as_bytes());
    let region_id = region.get_id();

    let r = new_compute_hash_request();
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    let r = new_verify_hash_request(vec![7, 8, 9, 0], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    cluster.must_put(b"k2", b"v2");
    cluster.shutdown();
}

#[test]
fn test_old_compact_log() {
    // If we just return None for CompactLog, the region state in ApplyFsm will
    // change. Because there is no rollback in new implementation.
    // This is a ERROR state.
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);
    cluster.run();

    // We don't return Persist after handling CompactLog.
    fail::cfg("no_persist_compact_log", "return").unwrap();
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();
    let prev_state = collect_all_states(&cluster, region_id);
    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    // Wait for state applys.
    std::thread::sleep(std::time::Duration::from_secs(2));

    let new_state = collect_all_states(&cluster, region_id);
    must_altered_memory_apply_state(&prev_state, &new_state);
    must_unaltered_disk_apply_state(&prev_state, &new_state);

    fail::remove("no_persist_compact_log");
    cluster.shutdown();
}

#[test]
fn test_compact_log() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    disable_auto_gen_compact_log(&mut cluster);

    cluster.run();

    cluster.must_put(b"k", b"v");
    let region = cluster.get_region("k".as_bytes());
    let region_id = region.get_id();

    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    fail::cfg("try_flush_data", "return(0)").unwrap();
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    std::thread::sleep(std::time::Duration::from_millis(500));
    let prev_state = collect_all_states(&cluster, region_id);

    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    // compact index should less than applied index
    assert!(!res.get_header().has_error(), "{:?}", res);

    // TODO(tiflash) Make sure compact log is filtered successfully.
    // Can be abstract to a retry function.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // CompactLog is filtered, because we can't flush data.
    // However, we can still observe apply index advanced
    let new_state = collect_all_states(&cluster, region_id);
    for i in prev_state.keys() {
        let old = prev_state.get(i).unwrap();
        let new = new_state.get(i).unwrap();
        assert_eq!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
        assert_eq!(
            old.in_disk_apply_state.get_truncated_state(),
            new.in_disk_apply_state.get_truncated_state()
        );
        assert_eq!(
            old.in_memory_apply_state.get_applied_index() + 1,
            new.in_memory_apply_state.get_applied_index()
        );
        // Persist is before.
        assert_eq!(
            old.in_disk_apply_state.get_applied_index(),
            new.in_disk_apply_state.get_applied_index()
        );
    }

    fail::remove("on_empty_cmd_normal");
    fail::remove("try_flush_data");

    let (compact_index, compact_term) = get_valid_compact_index(&new_state);
    let prev_state = new_state;
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);

    cluster.must_put(b"kz", b"vz");
    check_key(&cluster, b"kz", b"vz", Some(true), None, None);

    // CompactLog is not filtered
    let new_state = collect_all_states(&cluster, region_id);
    for i in prev_state.keys() {
        let old = prev_state.get(i).unwrap();
        let new = new_state.get(i).unwrap();
        assert_ne!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
        assert_eq!(
            old.in_memory_apply_state.get_applied_index() + 2, // compact log + (kz,vz)
            new.in_memory_apply_state.get_applied_index()
        );
    }

    cluster.shutdown();
}

#[test]
fn test_empty_cmd() {
    // Test if a empty command can be observed when leadership changes.
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);
    // Disable compact log
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();
    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());
    std::thread::sleep(std::time::Duration::from_secs(2));

    check_key(&cluster, b"k1", b"v1", Some(true), None, None);
    let prev_states = collect_all_states(&cluster, region_id);

    // We need forward empty cmd generated by leadership changing to TiFlash.
    cluster.must_transfer_leader(region.get_id(), peer_2.clone());
    std::thread::sleep(std::time::Duration::from_secs(2));

    let new_states = collect_all_states(&cluster, region_id);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_altered_memory_apply_term(&prev_states, &new_states);

    std::thread::sleep(std::time::Duration::from_secs(2));
    fail::cfg("on_empty_cmd_normal", "return").unwrap();

    let prev_states = new_states;
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());
    std::thread::sleep(std::time::Duration::from_secs(2));

    let new_states = collect_all_states(&cluster, region_id);
    must_unaltered_memory_apply_state(&prev_states, &new_states);
    must_unaltered_memory_apply_term(&prev_states, &new_states);

    fail::remove("on_empty_cmd_normal");

    cluster.shutdown();
}

#[test]
fn test_old_kv_write() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

    cluster.cfg.proxy_compat = false;
    // No persist will be triggered by CompactLog
    fail::cfg("no_persist_compact_log", "return").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k0", b"v0");
    // check_key(&cluster, b"k0", b"v0", Some(false), Some(false), None);

    // We can read initial raft state, since we don't persist meta either.
    let r1 = cluster.get_region(b"k0").get_id();
    let prev_states = collect_all_states(&mut cluster, r1);

    for i in 1..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we disable all observers, we can get nothing in either memory and disk.
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    let new_states = collect_all_states(&mut cluster, r1);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_unaltered_disk_apply_state(&prev_states, &new_states);

    debug!("now CompactLog can persist");
    fail::remove("no_persist_compact_log");

    let prev_states = collect_all_states(&mut cluster, r1);
    // Write more after we force persist when CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // We can read from mock-store's memory, we are not sure if we can read from
    // disk, since there may be or may not be a CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    // Force a compact log to persist.
    let region_r = cluster.get_region("k1".as_bytes());
    let region_id = region_r.get_id();
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req =
        test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);

    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(true),
            None,
        );
    }

    let new_states = collect_all_states(&mut cluster, r1);
    must_altered_memory_apply_state(&prev_states, &new_states);
    must_altered_disk_apply_state(&prev_states, &new_states);

    fail::remove("no_persist_compact_log");
    cluster.shutdown();
}
