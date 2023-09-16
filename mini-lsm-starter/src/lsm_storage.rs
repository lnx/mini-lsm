use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::RwLock;

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{map_bound, MemTable};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    path: PathBuf,
    block_cache: Arc<BlockCache>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1 << 20)),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = { self.inner.read().clone() };
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        for t in snapshot.imm_memtables.iter().rev() {
            if let Some(value) = t.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }
        let mut iters = Vec::new();
        for t in snapshot.l0_sstables.iter().rev() {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                t.clone(),
                key,
            )?));
        }
        let iter = MergeIterator::create(iters);
        if iter.is_valid() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        // FIXME: what about L1 - L6?
        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, b"");
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let flush_memtable;
        let sst_id;

        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
            flush_memtable = memtable.clone();
            sst_id = snapshot.next_sst_id;
            snapshot.imm_memtables.push(memtable);
            *guard = Arc::new(snapshot);
        }

        let mut builder = SsTableBuilder::new(4096);
        flush_memtable.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            snapshot.l0_sstables.push(sst);
            snapshot.next_sst_id += 1;
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = self.inner.read().clone();
        let mut memtable_iters = Vec::new();
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for t in snapshot.imm_memtables.iter().rev() {
            memtable_iters.push(Box::new(t.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::new();
        for t in snapshot.l0_sstables.iter().rev() {
            let iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(t.clone(), key)?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(t.clone(), key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(t.clone())?,
            };
            table_iters.push(Box::new(iter));
        }
        let table_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_iter, table_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
