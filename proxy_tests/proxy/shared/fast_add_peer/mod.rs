// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

mod fp;
mod simple;

use crate::utils::v1::{must_wait_until_cond_generic, ClusterExt, FFIHelperSet};

fn must_wait_fap_snapshot_removed(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_id: u64,
    peer_id: u64,
) {
    // Data and Region state are visible before the commit hook removes FAP data.
    must_wait_until_cond_generic(cluster_ext, region_id, Some(vec![store_id]), &|_| {
        let mut removed = false;
        cluster_ext.iter_ffi_helpers(Some(vec![store_id]), &mut |_, ffi: &mut FFIHelperSet| {
            removed = ffi
                .engine_store_server_helper
                .query_fap_snapshot_state(region_id, peer_id, 0, 0)
                == proxy_ffi::interfaces_ffi::FapSnapshotState::NotFound;
        });
        removed
    });
}
