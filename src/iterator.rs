use crate::db::Engine;
use crate::errors::Result;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let mut iter = self.iter(IteratorOptions::default());
        while let Some(item) = iter.next() {
            if !f(item.0, item.1) {
                break;
            }
        }
        Ok(())
    }
}

impl Iterator<'_> {
    pub fn rewind(&mut self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    pub fn seek(&mut self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    pub fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("fail to get value");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::Options;
    use crate::util;
    use std::path::PathBuf;

    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-list-keys");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let mut keys = engine.index.list_keys().expect("failed to get keys");
        assert!(keys.len() == 0);

        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("aade"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());
        let mut key2 = engine.list_keys().expect("failed to get key2");
        println!("{:?},{:}", key2, key2.len());
        assert!(key2.len() > 0);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-fold");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("aade"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        engine
            .fold(|keys, values| {
                println!("{:?}", keys);
                println!("{:?}", values);
                assert!(keys.len() > 0);
                assert!(values.len() > 0);
                true
            })
            .unwrap();
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-seek");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let mut iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        assert!(iter1.next().is_none());

        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let mut iter2 = engine.iter(IteratorOptions::default());
        iter2.seek("a".as_bytes().to_vec());
        assert!(iter2.next().is_some());

        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter3 = engine.iter(IteratorOptions::default());
        let res_r = iter3.seek("a".as_bytes().to_vec());
        assert_eq!(Bytes::from("aacc"), iter3.next().unwrap().0);
        //println!("{:?}", iter3.next().unwrap().0);
        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_next() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-next");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let mut iter1 = engine.iter(IteratorOptions::default());
        //println!("{:?}",iter1.next().unwrap().0);
        assert!(iter1.next().is_some());
        assert!(iter1.next().is_none());

        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());
        let mut iter2 = engine.iter(IteratorOptions::default());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_none());
        iter2.rewind();
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_some());
        assert!(iter2.next().is_none());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-prefix");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("aade"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter_opts = IteratorOptions::default();
        iter_opts.prefix = "aa".as_bytes().to_vec();
        let mut iter1 = engine.iter(iter_opts);
        while let Some(item) = iter1.next() {
            println!("{:?}, {:?}", item.0, item.1);
        }

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }
}
