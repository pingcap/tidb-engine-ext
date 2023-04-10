// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod hub_impls;
pub use hub_impls::*;
mod config;
pub use config::*;
pub(crate) mod engine_ext;
pub use engine_ext::*;
pub mod key_format;
mod proxy_ext;
pub use proxy_ext::*;
mod cached_region_info_manager;
pub use cached_region_info_manager::*;

use crate::{mixed_engine::write_batch::RocksWriteBatchVec, util::get_cf_handle};

pub fn do_write(cf: &str, key: &[u8]) -> bool {
    fail::fail_point!("before_tiflash_do_write", |_| true);
    match cf {
        engine_traits::CF_RAFT => true,
        engine_traits::CF_DEFAULT => {
            key == keys::PREPARE_BOOTSTRAP_KEY || key == keys::STORE_IDENT_KEY
        }
        _ => false,
    }
}

pub fn cf_to_name(batch: &RocksWriteBatchVec, cf: u32) -> &'static str {
    // d 0 w 2 l 1
    let handle_default = get_cf_handle(batch.db.as_ref(), engine_traits::CF_DEFAULT).unwrap();
    let d = handle_default.id();
    let handle_write = get_cf_handle(batch.db.as_ref(), engine_traits::CF_WRITE).unwrap();
    let w = handle_write.id();
    let handle_lock = get_cf_handle(batch.db.as_ref(), engine_traits::CF_LOCK).unwrap();
    let l = handle_lock.id();
    if cf == l {
        engine_traits::CF_LOCK
    } else if cf == w {
        engine_traits::CF_WRITE
    } else if cf == d {
        engine_traits::CF_DEFAULT
    } else {
        engine_traits::CF_RAFT
    }
}

#[cfg(any(test, feature = "testexport"))]
pub fn check_double_write(batch: &RocksWriteBatchVec) {
    // It will fire if we write by both observer(compat_old_proxy is not enabled)
    // and TiKV's WriteBatch.
    fail::fail_point!("before_tiflash_check_double_write", |_| {});
    tikv_util::debug!("check if double write happens");
    for wb in batch.wbs.iter() {
        for (_, cf, k, _) in wb.iter() {
            let handle = batch.db.cf_handle_by_id(cf as usize).unwrap();
            let cf_name = cf_to_name(batch, handle.id());
            match cf_name {
                engine_traits::CF_DEFAULT | engine_traits::CF_LOCK | engine_traits::CF_WRITE => {
                    assert!(crate::do_write(cf_name, k));
                }
                _ => (),
            };
        }
    }
}
#[cfg(not(any(test, feature = "testexport")))]
pub fn check_double_write(_: &RocksWriteBatchVec) {}

#[cfg(not(any(test, feature = "testexport")))]
pub fn log_check_double_write(_: &RocksWriteBatchVec) -> bool {
    false
}

#[cfg(any(test, feature = "testexport"))]
pub fn log_check_double_write(batch: &RocksWriteBatchVec) -> bool {
    check_double_write(batch);
    // TODO(tiflash) re-support this tracker.
    let mut e = true;
    for wb in batch.wbs.iter() {
        if !wb.is_empty() {
            e = false;
            break;
        }
    }
    if e {
        let bt = std::backtrace::Backtrace::capture();
        tikv_util::info!("abnormal empty write batch";
            "backtrace" => ?bt
        );
        // We don't return true here, since new version TiKV will not cause
        // deadlock here.
    }
    false
}
