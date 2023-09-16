use std::sync::Arc;

use anyhow::Result;

use crate::block::BlockIterator;
use crate::iterators::StorageIterator;

use super::SsTable;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_idx: usize,
    block_iterator: BlockIterator,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_idx = 0;
        let block = table.read_block(block_idx)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            block_idx,
            block_iterator,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_idx = 0;
        let block = self.table.read_block_cached(self.block_idx)?;
        self.block_iterator = BlockIterator::create_and_seek_to_first(block);
        self.block_iterator.seek_to_first();
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let mut block_idx = table.find_block_idx(key);
        let mut block = table.read_block(block_idx)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        if !block_iterator.is_valid() && block_idx + 1 < table.block_metas.len() {
            block_idx += 1;
            block = table.read_block(block_idx)?;
            block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        }
        Ok(Self {
            table,
            block_idx,
            block_iterator,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        self.block_idx = self.table.find_block_idx(key);
        let block = self.table.read_block_cached(self.block_idx)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        if !self.block_iterator.is_valid() && self.block_idx + 1 < self.table.block_metas.len() {
            self.block_idx += 1;
            let block = self.table.read_block_cached(self.block_idx)?;
            self.block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if self.block_iterator.is_valid() {
            Ok(())
        } else {
            if self.block_idx + 1 < self.table.block_metas.len() {
                self.block_idx += 1;
                let block = self.table.read_block_cached(self.block_idx)?;
                self.block_iterator = BlockIterator::create_and_seek_to_first(block);
            }
            Ok(())
        }
    }
}
