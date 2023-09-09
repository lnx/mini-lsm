use std::collections::BTreeMap;

use bytes::{BufMut, Bytes};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    cap: usize,
    size: usize,
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            cap: block_size,
            size: SIZEOF_U16,
            map: BTreeMap::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let new_size = if let Some(old) = self.map.get(key) {
            (self.size as isize + value.len() as isize - old.len() as isize) as usize
        } else {
            self.size + key.len() + value.len() + SIZEOF_U16 * 3
        };
        if new_size > self.cap && !self.is_empty() {
            return false;
        }
        self.map.insert(key.to_vec(), value.to_vec());
        self.size = new_size;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut offsets = Vec::new();
        let mut data = Vec::new();
        for (k, v) in self.map {
            offsets.push(data.len() as u16);
            data.put_u16(k.len() as u16);
            data.extend(k);
            data.put_u16(v.len() as u16);
            data.extend(v);
        }
        Block { offsets, data }
    }

    pub fn first_key(&self) -> Option<Bytes> {
        self.map.first_key_value().map(|(k, _)| k.clone().into())
    }
}
