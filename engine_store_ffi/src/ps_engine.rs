// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// Disable warnings for unused engine_rocks's feature.
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    fmt,
    fmt::{Debug, Formatter},
    mem, slice,
};

use byteorder::{BigEndian, ByteOrder};
use engine_traits::{
    Error, PerfContext, PerfContextExt, PerfContextKind, PerfLevel, RaftEngine, RaftEngineDebug,
    RaftEngineReadOnly, RaftLogBatch, RaftLogGcTask, Result,
};
use kvproto::{
    metapb::Region,
    raft_serverpb::{
        RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent, StoreRecoverState,
    },
};
use protobuf::Message;
use raft::eraftpb::Entry;
use tikv_util::{box_err, box_try, info};
use tracker::TrackerToken;

use crate::{gen_engine_store_server_helper, RawCppPtr};

// 1. STORE_IDENT 0
// 2. PREPARE_BOOTSTRAP 1
// 3. RaftLocalState 2
// 4. RegionLocalState 3
// 5. RaftApplyState 4
// 6. Snapshot RaftLocalState 5
// 7. Reserved 6..9
// 8. Log 10(+ offset 5)

// pub const PS_KEY_PREFIX: &[u8] = &[b'r', b'_'];
// pub const PS_KEY_SEP: u8 = b'_';
//
// const RAFT_LOCAL_STATE_ID : u64 = 2;
// const RAFT_LOG_ID_OFFSET : u64 = 5;
//
// pub fn ps_raft_state_key(region_id: u64) -> [u8; 19] {
//     let mut key = [0; 19];
//     key[..2].copy_from_slice(PS_KEY_PREFIX);
//     BigEndian::write_u64(&mut key[2..10], region_id);
//     key[10] = PS_KEY_SEP;
//     BigEndian::write_u64(&mut key[11..19], RAFT_LOCAL_STATE_ID);
//     key
// }
//
// pub fn ps_raft_log_key(region_id: u64, log_index: u64) -> [u8; 19] {
//     let mut key = [0; 19];
//     key[..2].copy_from_slice(PS_KEY_PREFIX);
//     BigEndian::write_u64(&mut key[2..10], region_id);
//     key[10] = PS_KEY_SEP;
//     BigEndian::write_u64(&mut key[11..19], log_index + RAFT_LOG_ID_OFFSET);
//     key
// }
//
// pub fn ps_raft_log_prefix(region_id: u64) -> [u8; 11] {
//     let mut key = [0; 11];
//     key[..2].copy_from_slice(PS_KEY_PREFIX);
//     BigEndian::write_u64(&mut key[2..10], region_id);
//     key[10] = PS_KEY_SEP;
//     key
// }
//
// pub fn ps_raft_log_index(key: &[u8]) -> u64 {
//     let expect_key_len = PS_KEY_PREFIX.len()
//         + mem::size_of::<u64>()
//         + mem::size_of::<u8>()
//         + mem::size_of::<u64>();
//     if key.len() != expect_key_len {
//         panic!("wrong key format {:?}", key);
//     }
//     BigEndian::read_u64(
//         &key[expect_key_len - mem::size_of::<u64>()..],
//     )
// }

pub struct PSEngineWriteBatch {
    pub engine_store_server_helper: isize,
    pub raw_write_batch: RawCppPtr,
}

impl PSEngineWriteBatch {
    pub fn new(engine_store_server_helper: isize) -> PSEngineWriteBatch {
        let helper = gen_engine_store_server_helper(engine_store_server_helper);
        let raw_write_batch = helper.create_write_batch();
        PSEngineWriteBatch {
            engine_store_server_helper,
            raw_write_batch,
        }
    }

    fn put_page(&mut self, page_id: &[u8], value: &[u8]) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.write_batch_put_page(self.raw_write_batch.ptr, page_id.into(), value.into());
        Ok(())
    }

    fn del_page(&mut self, page_id: &[u8]) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.write_batch_del_page(self.raw_write_batch.ptr, page_id.into());
        Ok(())
    }

    fn append_impl(
        &mut self,
        raft_group_id: u64,
        entries: &[Entry],
        mut ser_buf: Vec<u8>,
    ) -> Result<()> {
        for entry in entries {
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            let key = keys::raft_log_key(raft_group_id, entry.get_index());
            self.put_page(&key, &ser_buf)?;
        }
        Ok(())
    }

    fn put_msg<M: protobuf::Message>(&mut self, page_id: &[u8], m: &M) -> Result<()> {
        self.put_page(page_id, &m.write_to_bytes()?)
    }

    fn data_size(&self) -> usize {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        return helper.write_batch_size(self.raw_write_batch.ptr) as usize;
    }

    fn clear(&self) {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.write_batch_clear(self.raw_write_batch.ptr);
    }
}

impl RaftLogBatch for PSEngineWriteBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.append_impl(raft_group_id, &entries, ser_buf);
        }
        Ok(())
    }

    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64) {
        // This function is used to clean entries that will be overwritten
        // later. TODO: make sure overlapped entries will be overwritten
        // by newer log. for index in from..to {
        //     let key = ps_raft_log_key(raft_group_id, index);
        //     self.del_page(&key).unwrap();
        // }
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.put_msg(&keys::raft_state_key(raft_group_id), state)
    }

    fn persist_size(&self) -> usize {
        self.data_size()
    }

    fn is_empty(&self) -> bool {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.write_batch_is_empty(self.raw_write_batch.ptr) != 0
    }

    fn merge(&mut self, src: Self) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.write_batch_merge(self.raw_write_batch.ptr, src.raw_write_batch.ptr);
        Ok(())
    }

    fn put_store_ident(&mut self, ident: &StoreIdent) -> Result<()> {
        self.put_msg(keys::STORE_IDENT_KEY, ident)
    }

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()> {
        self.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region)
    }

    fn remove_prepare_bootstrap_region(&mut self) -> Result<()> {
        self.del_page(keys::PREPARE_BOOTSTRAP_KEY)
    }

    fn put_region_state(&mut self, raft_group_id: u64, state: &RegionLocalState) -> Result<()> {
        self.put_msg(&keys::region_state_key(raft_group_id), state)
    }

    fn put_apply_state(&mut self, raft_group_id: u64, state: &RaftApplyState) -> Result<()> {
        self.put_msg(&keys::apply_state_key(raft_group_id), state)
    }
}

#[derive(Clone)]
pub struct PSEngine {
    pub engine_store_server_helper: isize,
}

impl std::fmt::Debug for PSEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PSEngine")
            .field(
                "engine_store_server_helper",
                &self.engine_store_server_helper,
            )
            .finish()
    }
}

impl PSEngine {
    pub fn new() -> Self {
        PSEngine {
            engine_store_server_helper: 0,
        }
    }

    pub fn init(&mut self, engine_store_server_helper: isize) {
        self.engine_store_server_helper = engine_store_server_helper;
    }

    fn get_msg_cf<M: protobuf::Message + Default>(&self, page_id: &[u8]) -> Result<Option<M>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let value = helper.read_page(page_id.into());
        if value.view.len == 0 {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(unsafe {
            slice::from_raw_parts(value.view.data as *const u8, value.view.len as usize)
        })?;
        Ok(Some(m))
    }

    fn get_value(&self, page_id: &[u8]) -> Option<Vec<u8>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let value = helper.read_page(page_id.into());
        return if value.view.len == 0 {
            None
        } else {
            Some(value.view.to_slice().to_vec())
        };
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Option<Vec<u8>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let target_key = helper.seek_ps_key(key.into());
        if target_key.view.len == 0 {
            None
        } else {
            Some(target_key.view.to_slice().to_vec())
        }
    }

    /// scan the key between start_key(inclusive) and end_key(exclusive),
    /// the upper bound is omitted if end_key is empty
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let values = helper.scan_page(start_key.into(), end_key.into());
        for i in 0..values.len {
            let value = unsafe { &*values.inner.offset(i as isize) };
            if value.page_view.len != 0 {
                if !f(
                    &value.key_view.to_slice().to_vec(),
                    &value.page_view.to_slice().to_vec(),
                )? {
                    break;
                }
            }
        }
        Ok(())
    }

    fn gc_impl(&self, raft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
        if from == 0 {
            let start_key = keys::raft_log_key(raft_group_id, 0);
            let prefix = keys::raft_log_prefix(raft_group_id);
            // TODO: make sure the seek can skip other raft related key and to the first log
            // key
            match self.seek(&start_key) {
                Some(target_key) if target_key.starts_with(&prefix) => {
                    from = box_try!(keys::raft_log_index(&target_key))
                }
                // No need to gc.
                _ => return Ok(0),
            }
        }
        if from >= to {
            return Ok(0);
        }
        // info!("gc_impl raft_group_id {} from {} to {}", raft_group_id, from ,to);

        let mut raft_wb = self.log_batch(0);
        for idx in from..to {
            raft_wb.del_page(&keys::raft_log_key(raft_group_id, idx));
        }
        // TODO: keep the max size of raft_wb under some threshold
        self.consume(&mut raft_wb, false);
        Ok((to - from) as usize)
    }

    fn is_empty(&self) -> bool {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.is_ps_empty() != 0
    }
}

impl RaftEngineReadOnly for PSEngine {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        let key = keys::raft_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = keys::raft_log_key(raft_group_id, index);
        self.get_msg_cf(&key)
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

        let start_key = keys::raft_log_key(region_id, low);
        let end_key = keys::raft_log_key(region_id, high);

        self.scan(&start_key, &end_key, |_, page| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(page)?;
            buf.push(entry);
            total_size += page.len();
            count += 1;
            Ok(total_size < max_size)
        })?;

        return Ok(count);
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        let start_key = keys::raft_log_key(region_id, 0);
        let end_key = keys::raft_log_key(region_id, u64::MAX);
        self.scan(&start_key, &end_key, |_, page| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(page)?;
            buf.push(entry);
            Ok(true)
        })?;
        Ok(())
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(self.is_empty())
    }

    fn get_store_ident(&self) -> Result<Option<StoreIdent>> {
        self.get_msg_cf(keys::STORE_IDENT_KEY)
    }

    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>> {
        self.get_msg_cf(keys::PREPARE_BOOTSTRAP_KEY)
    }

    fn get_region_state(&self, raft_group_id: u64) -> Result<Option<RegionLocalState>> {
        let key = keys::region_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_apply_state(&self, raft_group_id: u64) -> Result<Option<RaftApplyState>> {
        let key = keys::apply_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_recover_state(&self) -> Result<Option<StoreRecoverState>> {
        self.get_msg_cf(keys::RECOVER_STATE_KEY)
    }
}

impl RaftEngineDebug for PSEngine {
    fn scan_entries<F>(&self, raft_group_id: u64, mut f: F) -> Result<()>
    where
        F: FnMut(&Entry) -> Result<bool>,
    {
        let start_key = keys::raft_log_key(raft_group_id, 0);
        let end_key = keys::raft_log_key(raft_group_id, u64::MAX);
        self.scan(&start_key, &end_key, |_, value| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(value)?;
            f(&entry)
        });
        Ok(())
    }
}

impl RaftEngine for PSEngine {
    type LogBatch = PSEngineWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        PSEngineWriteBatch::new(self.engine_store_server_helper)
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        let bytes = batch.data_size();
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.consume_write_batch(batch.raw_write_batch.ptr);
        batch.clear();
        Ok(bytes)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        self.consume(batch, sync_log)
    }

    fn clean(
        &self,
        raft_group_id: u64,
        mut first_index: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        // info!("try clean raft_group_id {} from {} to {}", raft_group_id, first_index,
        // state.last_index);
        batch.del_page(&keys::raft_state_key(raft_group_id))?;
        batch.del_page(&keys::region_state_key(raft_group_id))?;
        batch.del_page(&keys::apply_state_key(raft_group_id))?;
        if first_index == 0 {
            let start_key = keys::raft_log_key(raft_group_id, 0);
            let prefix = keys::raft_log_prefix(raft_group_id);
            // TODO: make sure the seek can skip other raft related key and to the first log
            // key
            match self.seek(&start_key) {
                Some(target_key) if target_key.starts_with(&prefix) => {
                    first_index = box_try!(keys::raft_log_index(&target_key))
                }
                // No need to gc.
                _ => return Ok(()),
            }
        }
        if first_index >= state.last_index {
            return Ok(());
        }
        info!(
            "clean raft_group_id {} from {} to {}",
            raft_group_id, first_index, state.last_index
        );
        // TODO: find the first raft log index of this raft group
        if first_index <= state.last_index {
            for index in first_index..=state.last_index {
                batch.del_page(&keys::raft_log_key(raft_group_id, index));
            }
        }
        self.consume(batch, true);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut wb = self.log_batch(0);
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let buf = Vec::with_capacity(max_size as usize);
            wb.append_impl(raft_group_id, &entries, buf)?;
            return self.consume(&mut wb, false);
        }
        Ok(0)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        let mut wb = self.log_batch(0);
        wb.put_msg(&keys::raft_state_key(raft_group_id), state);
        self.consume(&mut wb, false);
        Ok(())
    }

    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize> {
        self.gc_impl(raft_group_id, from, to)
    }

    fn batch_gc(&self, groups: Vec<RaftLogGcTask>) -> Result<usize> {
        let mut total = 0;
        for task in groups {
            total += self.gc(task.raft_group_id, task.from, task.to)?;
        }
        Ok(total)
    }

    fn flush_metrics(&self, instance: &str) {}

    fn reset_statistics(&self) {}

    fn dump_stats(&self) -> Result<String> {
        Ok(String::from(""))
    }

    fn get_engine_path(&self) -> &str {
        ""
    }

    fn get_engine_size(&self) -> Result<u64> {
        Ok(0)
    }

    fn put_store_ident(&self, ident: &StoreIdent) -> Result<()> {
        let mut wb = self.log_batch(0);
        wb.put_msg(keys::STORE_IDENT_KEY, ident);
        self.consume(&mut wb, false);
        Ok(())
    }

    fn for_each_raft_group<E, F>(&self, f: &mut F) -> std::result::Result<(), E>
    where
        F: FnMut(u64) -> std::result::Result<(), E>,
        E: From<Error>,
    {
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut err = None;
        self.scan(start_key, end_key, |key, _| {
            let (region_id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            match f(region_id) {
                Ok(()) => Ok(true),
                Err(e) => {
                    err = Some(e);
                    Ok(false)
                }
            }
        })?;
        match err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    fn put_recover_state(&self, state: &StoreRecoverState) -> Result<()> {
        let mut wb = self.log_batch(0);
        wb.put_msg(keys::RECOVER_STATE_KEY, state);
        self.consume(&mut wb, false);
        Ok(())
    }
}

impl PerfContextExt for PSEngine {
    type PerfContext = PSPerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        PSPerfContext::new(level, kind)
    }
}

#[derive(Debug)]
pub struct PSPerfContext {}

impl PSPerfContext {
    pub fn new(level: PerfLevel, kind: PerfContextKind) -> Self {
        PSPerfContext {}
    }
}

impl PerfContext for PSPerfContext {
    fn start_observe(&mut self) {}

    fn report_metrics(&mut self, trackers: &[TrackerToken]) {}
}
