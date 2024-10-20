use crate::data::log_record::{decode_log_record_pos, LogRecodPos};
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;
use bytes::Bytes;
use jammdb::{Error, DB};
use std::path::PathBuf;
use std::sync::Arc;

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bptree-bucket";

pub struct BPlusTree {
    tree: Arc<DB>,
}

pub struct BPTreeIterator {
    items: Vec<(Vec<u8>, LogRecodPos)>,
    current_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for BPTreeIterator {
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

impl BPlusTree {
    pub fn new(dir_path: PathBuf) -> Self {
        let bptree = DB::open(dir_path.join(BPTREE_INDEX_FILE_NAME)).expect("fail to open bptree");
        let tree = Arc::new(bptree);
        let tx = tree.tx(true).expect("fail to create tx object");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();
        Self { tree: tree.clone() }
    }
}

impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, pos: LogRecodPos) -> Some(LogRecodPos){
        let mut result=None;
        let tx = self.tree.tx(true).expect("fail to create tx object");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        if let Some(kv)=bucket.get_kv(&key) {
            let pos=decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }

        bucket.put(key, pos.encode()).expect("fail to put key");
        tx.commit().unwrap();
        result
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecodPos> {
        let tx = self.tree.tx(false).expect("fail to create tx object");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecodPos> {
        let mut result=None;
        let tx = self.tree.tx(true).expect("fail to create tx object");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Ok(kv) = bucket.delete(key) {
            let pos=decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }
        tx.commit().unwrap();
        result
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<Bytes>> {
        let tx = self.tree.tx(false).expect("fail to create tx object");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        let mut keys = Vec::new();
        for data in bucket.cursor() {
            keys.push(Bytes::copy_from_slice(data.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, iterator_options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::new();
        let tx = self.tree.tx(true).expect("fail to create tx object");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        for data in bucket.cursor() {
            let key = data.key().to_vec();
            let pos = decode_log_record_pos(data.kv().value().to_vec());
            items.push((key, pos));
        }

        if iterator_options.reverse {
            items.reverse();
        }

        Box::new(BPTreeIterator {
            items,
            current_index: 0,
            options: iterator_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[test]
    fn test_bptree_put() {
        let path = PathBuf::from("/tmp/bptree-test");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path);
        let res1 = bpt.put(
            "aabc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 100,
            },
        );
        assert!(res1);
        let res2 = bpt.put(
            "bbbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 101,
            },
        );
        assert!(res2);

        let res3 = bpt.put(
            "ccbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 3,
                offset: 101,
            },
        );
        assert!(res3);

        let res4 = bpt.put(
            "ddbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 4,
                offset: 101,
            },
        );
        assert!(res4);
        println!("{:?}", bpt.list_keys().unwrap());
    }
    #[test]
    fn test_bptree_get() {
        let path = PathBuf::from("/tmp/bptree-test");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path);
        let res1 = bpt.put(
            "aabc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 100,
            },
        );
        assert!(res1);
        let res2 = bpt.put(
            "bbbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 101,
            },
        );
        assert!(res2);

        let res3 = bpt.put(
            "ccbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 3,
                offset: 101,
            },
        );
        assert!(res3);

        let res4 = bpt.put(
            "ddbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 4,
                offset: 101,
            },
        );
        assert!(res4);
        let get_res1 = bpt.get("aabc".as_bytes().to_vec());
        assert!(get_res1.is_some());
        let get_res2 = bpt.get("bbbc".as_bytes().to_vec());
        assert!(get_res2.is_some());
        let get_res3 = bpt.get("cccbc".as_bytes().to_vec());
        assert!(get_res3.is_none());
        println!("{:?}", bpt.list_keys().unwrap());
    }

    #[test]
    fn test_bptree_delete() {
        let path = PathBuf::from("/tmp/bptree-test");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path);
        let res1 = bpt.put(
            "aabc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 100,
            },
        );
        assert!(res1);
        let res2 = bpt.put(
            "bbbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 101,
            },
        );
        assert!(res2);

        let res3 = bpt.put(
            "ccbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 3,
                offset: 101,
            },
        );
        assert!(res3);

        let res4 = bpt.put(
            "ddbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 4,
                offset: 101,
            },
        );
        assert!(res4);

        let delete_res1 = bpt.delete("aabc".as_bytes().to_vec());
        assert!(delete_res1);
        let delete_res2 = bpt.delete("bbbc".as_bytes().to_vec());
        assert!(delete_res2);

        println!("{:?}", bpt.list_keys().unwrap());
    }
    #[test]
    fn test_bptree_list_keys() {
        let path = PathBuf::from("/tmp/bptree-test");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path);
        let res1 = bpt.put(
            "aabc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 100,
            },
        );
        assert!(res1);
        let res2 = bpt.put(
            "bbbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 101,
            },
        );
        assert!(res2);

        let res3 = bpt.put(
            "ccbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 3,
                offset: 101,
            },
        );
        assert!(res3);

        let res4 = bpt.put(
            "ddbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 4,
                offset: 101,
            },
        );
        assert!(res4);

        println!("{:?}", bpt.list_keys().unwrap());
    }
    #[test]
    fn test_bptree_iterator() {
        let path = PathBuf::from("/tmp/bptree-test");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path);
        let res1 = bpt.put(
            "aabc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 1,
                offset: 100,
            },
        );
        assert!(res1);
        let res2 = bpt.put(
            "bbbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 2,
                offset: 101,
            },
        );
        assert!(res2);

        let res3 = bpt.put(
            "ccbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 3,
                offset: 101,
            },
        );
        assert!(res3);

        let res4 = bpt.put(
            "ddbc".as_bytes().to_vec(),
            LogRecodPos {
                file_id: 4,
                offset: 101,
            },
        );
        assert!(res4);

        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter = bpt.iterator(opts);
        while let Some((k, v)) = iter.next() {
            println!("{:?}, {:?}", k, v);
        }
    }
}
