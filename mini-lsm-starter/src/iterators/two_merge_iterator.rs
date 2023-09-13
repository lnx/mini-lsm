use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b };
        iter.skip_b()?;
        Ok(iter)
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            while self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?;
            }
        }
        Ok(())
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if !self.a.is_valid() {
            return self.b.key();
        }
        if !self.b.is_valid() {
            return self.a.key();
        }
        if self.a.key() <= self.b.key() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if !self.a.is_valid() {
            return self.b.value();
        }
        if !self.b.is_valid() {
            return self.a.value();
        }
        if self.a.key() <= self.b.key() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.a.is_valid() {
            return self.b.next();
        }
        if !self.b.is_valid() {
            return self.a.next();
        }
        if self.a.key() <= self.b.key() {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        Ok(())
    }
}
