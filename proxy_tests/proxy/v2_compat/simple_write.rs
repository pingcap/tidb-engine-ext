// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use test_raftstore_v2::Simulator as S2;

use super::utils::*;
use crate::utils::v1::*;

#[test]
fn test_write_simple() {
    const SPLIT_REGION_ID: u64 = 1000;
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(1, 2);
    let (mut cluster_v1, _) = new_mock_cluster(1, 2);

    cluster_v1
        .cfg
        .server
        .labels
        .insert(String::from("engine"), String::from("tiflash"));
    cluster_v1.cfg.tikv.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.pd_client.disable_default_operator();
    cluster_v2.pd_client.disable_default_operator();
    let r11 = cluster_v1.run_conf_change();
    let r21 = cluster_v2.run_conf_change();

    cluster_v1.must_put(b"k0", b"v0");
    cluster_v2.must_put(b"k0", b"v0");
    cluster_v1
        .pd_client
        .must_add_peer(r11, new_learner_peer(2, 10));
    cluster_v2
        .pd_client
        .must_add_peer(r21, new_learner_peer(2, 10));

    check_key(&cluster_v1, b"k0", b"v0", Some(true), None, None);
    let trans1 = Mutex::new(cluster_v1.sim.read().unwrap().get_router(2).unwrap());
    let trans2 = Mutex::new(cluster_v2.sim.read().unwrap().get_router(1).unwrap());

    // Keep the unused V2 split learner behind, so duplicating bridge traffic
    // would expose one peer's acknowledgements advancing another peer's commit.
    cluster_v2.add_recv_filter_on_node(
        2,
        Box::new(
            test_raftstore::RegionPacketFilter::new(SPLIT_REGION_ID, 2)
                .direction(test_raftstore::Direction::Recv)
                .msg_type(MessageType::MsgAppend),
        ),
    );

    // This fixture joins V2 leader 1 with V1 learner 2. Replace each cluster's
    // original transport instead of also delivering to its same-ID peer.
    let factory1 = ForwardFactoryV1 {
        node_id: 1,
        chain_send: Arc::new(move |m| {
            info!("send to trans2"; "msg" => ?m);
            let _ = trans2.lock().unwrap().send_raft_message(Box::new(m));
        }),
        keep_msg: false,
    };
    cluster_v1.add_send_filter(factory1);
    let factory2 = ForwardFactoryV2 {
        node_id: 2,
        chain_send: Arc::new(move |m| {
            info!("send to trans1"; "msg" => ?m);
            let _ = trans1.lock().unwrap().send_raft_message(m);
        }),
        keep_msg: false,
    };
    cluster_v2.add_send_filter(factory2);

    cluster_v2.must_put(b"k1", b"v1");
    assert_eq!(
        cluster_v2.must_get(b"k1").unwrap(),
        "v1".as_bytes().to_vec()
    );
    check_key(
        &cluster_v1,
        b"k1",
        b"v1",
        Some(true),
        Some(true),
        Some(vec![2]),
    );

    cluster_v2.must_split(&cluster_v2.get_region(b"k1"), b"k1");

    let left = cluster_v2.get_region(b"k0");
    let right = cluster_v2.get_region(b"k1");
    assert_eq!(left.get_id(), SPLIT_REGION_ID);
    assert_eq!(right.get_id(), r21);
    for (key, previous, new_key, value, region_id) in [
        (
            b"k0",
            b"v0",
            b"k00".as_slice(),
            b"left".as_slice(),
            left.get_id(),
        ),
        (
            b"k1",
            b"v1",
            b"k2".as_slice(),
            b"right".as_slice(),
            right.get_id(),
        ),
    ] {
        check_key_ex(
            &cluster_v1,
            key,
            previous,
            Some(true),
            Some(true),
            Some(vec![2]),
            Some(region_id),
            true,
        );
        // Normal mock writes are buffered; check that each split peer receives
        // a new key in memory without requiring an unrelated flush trigger.
        cluster_v2.must_put(new_key, value);
        check_key_ex(
            &cluster_v1,
            new_key,
            value,
            Some(true),
            None,
            Some(vec![2]),
            Some(region_id),
            true,
        );
    }

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}
