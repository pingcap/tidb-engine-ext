// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::proxy::*;

#[derive(PartialEq, Eq)]
enum SourceType {
    Leader,
    Learner,
    DelayedLearner,
    InvalidSource,
}

fn simple_fast_add_peer(source_type: SourceType, block_wait: bool) {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_persist_with_finish", "return").unwrap();
    fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    if block_wait {
        fail::cfg("ffi_fast_add_peer_block_wait", "return(1)").unwrap();
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

    // Add learner 3 according to source_type
    match source_type {
        SourceType::Learner | SourceType::DelayedLearner => {
            fail::cfg("ffi_fast_add_peer_from_id", "return(2)").unwrap();
        }
        SourceType::InvalidSource => {
            fail::cfg("ffi_fast_add_peer_from_id", "return(100)").unwrap();
        }
        _ => (),
    };

    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    // std::thread::sleep(std::time::Duration::from_millis(2000));
    // match source_type {
    //     SourceType::Learner => {
    //         // Wait until Learner has applied ConfChange
    //         must_wait_until_cond_node(&cluster, 1, Some(vec![2]), &|states:
    // &States| -> bool {
    // find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
    //         });
    //     }
    //     _ => {},
    // }
    cluster.must_put(b"k2", b"v2");

    match source_type {
        SourceType::DelayedLearner => {
            // Make sure conf change is applied.
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
            cluster.add_send_filter(CloneFilterFactory(
                RegionPacketFilter::new(1, 2)
                    .msg_type(MessageType::MsgAppend)
                    .direction(Direction::Recv),
            ));
            cluster.must_put(b"k3", b"v3");
        }
        _ => (),
    };

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

    match source_type {
        SourceType::DelayedLearner => {
            cluster.clear_send_filters();
        }
        _ => (),
    };

    fail::remove("ffi_fast_add_peer_from_id");
    fail::remove("on_pre_persist_with_finish");
    fail::remove("ffi_fast_add_peer_block_wait");
    cluster.shutdown();
}

#[test]
fn test_fast_add_peer_from_leader() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Leader, false);
    fail::remove("on_pre_persist_with_finish");
}

/// Fast path by learner snapshot.
#[test]
fn test_fast_add_peer_from_learner() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, false);
    fail::remove("on_pre_persist_with_finish");
}

/// If a learner is delayed, but already applied ConfChange.
#[test]
fn test_fast_add_peer_from_delayed_learner() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, false);
    fail::remove("on_pre_persist_with_finish");
}

/// If we select a wrong source, or we can't run fast path, we can fallback to
/// normal.
#[test]
fn test_fast_add_peer_from_invalid_source() {
    simple_fast_add_peer(SourceType::InvalidSource, false);
}

#[test]
fn test_fast_add_peer_from_learner_blocked() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::Learner, true);
    fail::remove("on_pre_persist_with_finish");
}

#[test]
fn test_fast_add_peer_from_delayed_learner_blocked() {
    fail::cfg("fallback_to_slow_path_not_allow", "panic").unwrap();
    simple_fast_add_peer(SourceType::DelayedLearner, true);
    fail::remove("on_pre_persist_with_finish");
}
