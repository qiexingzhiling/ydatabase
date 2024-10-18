use crate::data::log_record::LogRecodPos;
use crate::index::btree::BTreeIterator;
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::ops::Index;
use std::sync::Arc;

pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecodPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecodPos)>,
    current_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for SkipListIterator {
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

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecodPos) -> bool {
        self.skl.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecodPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let remove_res = self.skl.remove(&key);
        remove_res.is_some()
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<Bytes>> {
        let mut key = Vec::with_capacity(self.skl.len());
        for e in self.skl.iter() {
            key.push(Bytes::copy_from_slice(e.key()));
        }
        Ok(key)
    }

    fn iterator(&self, iterator_options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());

        for e in self.skl.iter() {
            items.push((e.key().clone(), *e.value()));
        }
        if iterator_options.reverse {
            items.reverse();
        }
        Box::new(SkipListIterator {
            items,
            current_index: 0,
            options: iterator_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::read_to_string;
    #[test]
    fn test_skiplist_put() {
        let mut skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "bbcd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "cccd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "cced".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1124,
                offset: 1112,
            },
        );
        assert!(res4);
    }

    #[test]
    fn test_skiplist_get() {
        let mut skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "bbcd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "cccd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "cced".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1124,
                offset: 1112,
            },
        );
        assert!(res4);

        let get_res1 = skl.get("aacd".as_bytes().to_vec());
        assert!(get_res1.is_some());
        println!("{:?}", get_res1);

        let get_res2 = skl.get("bbcd".as_bytes().to_vec());
        assert!(get_res2.is_some());
        println!("{:?}", get_res2);

        let get_res3 = skl.get("abcd".as_bytes().to_vec());
        assert!(get_res3.is_none());
        println!("{:?}", get_res3);
    }

    #[test]
    fn test_skiplist_delete() {
        let mut skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "bbcd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "cccd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "cced".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1124,
                offset: 1112,
            },
        );
        assert!(res4);

        let delete_res1 = skl.delete("aacd".as_bytes().to_vec());
        assert!(delete_res1);
        let delete_res2 = skl.delete("bbcd".as_bytes().to_vec());
        assert!(delete_res2);
        println!("{:#?}", skl.list_keys());
    }
    #[test]
    fn test_skiplist_list_keys() {
        let mut skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "bbcd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "cccd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "cced".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1124,
                offset: 1112,
            },
        );
        assert!(res4);
        println!("{:#?}", skl.list_keys());
    }
    #[test]
    fn test_skiplist_iterator() {
        let mut skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "bbcd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "cccd".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "cced".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1124,
                offset: 1112,
            },
        );
        assert!(res4);

        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter = skl.iterator(opts);
        while let Some((k, v)) = iter.next() {
            println!("{:?}, {:?}", k, v);
        }
    }
}
