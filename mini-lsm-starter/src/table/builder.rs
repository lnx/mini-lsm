use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use crate::block::{Block, BlockBuilder};
use crate::lsm_storage::BlockCache;
use crate::table::FileObject;

use super::{BlockMeta, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) data: Vec<Block>,
    pub(super) meta: Vec<BlockMeta>,
    block_builder: BlockBuilder,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let ok = self.block_builder.add(key, value);
        if !ok {
            let mut new_builder = BlockBuilder::new(self.block_size);
            if !new_builder.add(key, value) {
                unreachable!();
            }
            let old_builder = std::mem::replace(&mut self.block_builder, new_builder);
            self.meta.push(BlockMeta {
                offset: self.estimated_size(),
                first_key: old_builder.first_key().unwrap(),
            });
            self.data.push(old_builder.build());
        }
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.iter().map(|b| b.size()).sum()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.block_builder.is_empty() {
            self.meta.push(BlockMeta {
                offset: self.estimated_size(),
                first_key: self.block_builder.first_key().unwrap(),
            });
            self.data.push(self.block_builder.build());
        }

        let mut data = Vec::new();
        for b in self.data {
            data.extend(b.encode());
        }
        let block_meta_offset = data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.put_u32(block_meta_offset as u32);
        Ok(SsTable {
            file: FileObject(data.into()),
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
