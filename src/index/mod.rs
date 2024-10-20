mod bptree;
mod btree;
mod skiplist;

use crate::data::log_record::LogRecodPos;
use crate::errors::Result;
use crate::index;
use crate::options::IndexType::SkipList;
use crate::options::{IndexType, IteratorOptions, Options};
use bytes::Bytes;
use std::path::PathBuf;

pub trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecodPos) -> Option<LogRecodPos>;
    fn get(&self, key: Vec<u8>) -> Option<LogRecodPos>;
    fn delete(&self, key: Vec<u8>) -> Option<LogRecodPos>;

    fn list_keys(&self) -> Result<Vec<Bytes>>;

    fn iterator(&self, iterator_options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        SkipList => Box::new(skiplist::SkipList::new()),
        IndexType::BPlusTree => Box::new(bptree::BPlusTree::new(dir_path)),
        _ => panic!("unknown index type"),
    }
}

pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecodPos)>;
}
