// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_traits::{IterOptions, Iterable, Iterator, Peekable};
use mock_engine_store;
use test_raftstore::*;

#[test]
fn test_normal() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(0, 3, sim, pd_client);

    cluster.run();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    for id in cluster.engines.keys() {
        must_get_equal(&cluster.get_engine(*id), k, v);
        // must_get_equal(db, k, v);
    }

    cluster.shutdown();
}
