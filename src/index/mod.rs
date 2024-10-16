mod btree;

use crate::data::log_record::LogRecodPos;
use crate::index;
use crate::options::IndexType::SkipList;
use crate::options::{IndexType, IteratorOptions, Options};
use std::path::PathBuf;
use bytes::Bytes;
use crate::errors::Result;

pub trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecodPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecodPos>;
    fn delete(&self, key: Vec<u8>) -> bool;

    fn list_keys(&self) -> Result<Vec<Bytes>>;

    fn iterator(&self, iterator_options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        SkipList => todo!(),
        _ => panic!("unknown index type"),
    }
}

pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecodPos)>;

}
