use crate::data::log_record::LogRecodPos;
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecodPos>>>,
}

impl BTree {
    pub fn new() -> BTree {
        BTree {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecodPos) -> Option<LogRecodPos> {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos)
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecodPos> {
        let mut read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecodPos> {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key)
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());

        for (k, v) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&k));
        }
        Ok(keys)
    }

    fn iterator(&self, iterator_options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());

        for (k, v) in read_guard.iter() {
            items.push((k.clone(), v.clone()));
        }
        if iterator_options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            current_index: 0,
            options: iterator_options,
        })
    }
}

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecodPos)>,
    current_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.current_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.current_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(index) => index,
            Err(index) => index,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecodPos)> {
        if self.current_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.current_index) {
            self.current_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res = bt.put(
            "".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res.is_none(), true);
        let re1 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 20,
            },
        );
        assert_eq!(re1.is_none(), true);
    }
    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res = bt.put(
            "".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res.is_some(), true);
        let re1 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 20,
            },
        );
        assert_eq!(re1.is_some(), true);

        let pos1 = bt.get("aa".as_bytes().to_vec());
        //println!("{:?}", pos1);
        let pos2 = bt.get("".as_bytes().to_vec());
        //println!("{:?}", pos2);
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 2);
        assert_eq!(pos1.unwrap().offset, 20);
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 1);
        assert_eq!(pos2.unwrap().offset, 10);
    }
    #[test]
    fn test_btree_delete() {
        let bt = BTree::new();
        bt.put(
            "".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aa".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 20,
            },
        );
        let res1 = bt.delete("".as_bytes().to_vec());
        assert_eq!(res1.is_some(), true);
        let res2 = bt.delete("aa".as_bytes().to_vec());
        assert_eq!(res2.is_some(), true);
    }
    #[test]
    fn test_btree_iterator_seek() {
        let bt = BTree::new();
        let mut iter1 = bt.iterator(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        let res = iter1.next();
        //println!("{:?}", res);
        assert_eq!(res.is_none(), true);

        bt.put(
            "ccde".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter2 = bt.iterator(IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res2 = iter2.next();
        //println!("{:?}",res2);
        assert_eq!(res2.is_some(), true);
        iter2.seek("ee".as_bytes().to_vec());
        let res3 = iter2.next();
        //println!("{:?}",res3);
        assert_eq!(res3.is_none(), true);

        bt.put(
            "bbde".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aade".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cade".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter3 = bt.iterator(IteratorOptions::default());
        iter3.seek("b".as_bytes().to_vec());
        while let Some(item) = iter3.next() {
            //println!("{:?}",String::from_utf8(item.0.clone()).unwrap());
        }

        let mut iter4 = bt.iterator(IteratorOptions::default());
        iter4.seek("cade".as_bytes().to_vec());
        let res4 = iter4.next();
        //println!("{:?}",res4);

        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let mut iter5 = bt.iterator(iter_opts);
        iter5.seek("bb".as_bytes().to_vec());
        while let Some(item) = iter5.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }
    #[test]
    fn test_btree_iterator_next() {
        let bt = BTree::new();
        let mut iter1 = bt.iterator(IteratorOptions::default());
        //assert!(iter1.next().is_none());
        bt.put(
            "bbde".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aade".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cade".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 10,
            },
        );
        println!("{:?}", iter1.next());

        let mut opt1 = IteratorOptions::default();
        opt1.reverse = true;
        let mut iter2 = bt.iterator(opt1);
        while let Some(item) = iter2.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut opt2 = IteratorOptions::default();
        opt2.prefix = "c".as_bytes().to_vec();
        let mut iter3 = bt.iterator(opt2);
        while let Some(item) = iter3.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut opt3 = IteratorOptions::default();
        opt3.prefix = "cssssss".as_bytes().to_vec();
        let mut iter4 = bt.iterator(opt3);
        while let Some(item) = iter4.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }
}
