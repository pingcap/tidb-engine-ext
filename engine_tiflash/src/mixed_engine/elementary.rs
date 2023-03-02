// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngineIterator;
use engine_traits::{IterOptions, ReadOptions, Result};

use super::{write_batch::MixedWriteBatch, MixedDbVector};
pub trait ElementaryEngine: std::fmt::Debug {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()>;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<MixedDbVector>>;

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<MixedDbVector>>;

    #[allow(clippy::type_complexity)]
    fn scan(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<RocksEngineIterator>;

    fn write_batch(&self) -> MixedWriteBatch;

    fn write_batch_with_cap(&self, cap: usize) -> MixedWriteBatch;
}
