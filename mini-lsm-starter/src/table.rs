use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
use bytes::{Buf, BufMut, Bytes};

pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

mod builder;
mod iterator;

pub const SIZEOF_U32: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for m in block_meta {
            buf.put_u32(m.offset as u32);
            buf.put_u32(m.first_key.len() as u32);
            buf.extend(&m.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut v = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u32() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            v.push(BlockMeta { offset, first_key });
        }
        v
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        let mut file = File::create(path)?;
        file.write_all(&data)?;
        Ok(Self(data.into()))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(Self(data.into()))
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let block_meta_offset = (&(file
            .read(file.size() - SIZEOF_U32 as u64, SIZEOF_U32 as u64)?)[..])
            .get_u32() as usize;
        let len = file.size() - block_meta_offset as u64 - SIZEOF_U32 as u64;
        let block_metas =
            BlockMeta::decode_block_meta(&file.read(block_meta_offset as u64, len)?[..]);
        Ok(Self {
            file,
            block_metas,
            block_meta_offset,
            id,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_meta = self
            .block_metas
            .get(block_idx)
            .ok_or_else(|| anyhow!("invalid block_idx: {:?}", block_idx))?;
        let offset = block_meta.offset as u64;
        let len = if block_idx + 1 < self.block_metas.len() {
            self.block_metas[block_idx + 1].offset as u64 - offset
        } else {
            self.block_meta_offset as u64 - offset
        };
        let data = self.file.read(offset, len)?;
        let block = Block::decode(&data);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut lo = 0;
        let mut hi = self.block_metas.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match &self.block_metas[mid].first_key[..].cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return mid,
            }
        }
        if hi > 0 {
            hi - 1
        } else {
            0
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
