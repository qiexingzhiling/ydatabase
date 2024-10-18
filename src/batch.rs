use crate::data::log_record::{LogRecodType, LogRecord};
use crate::db::Engine;
use crate::errors::{Errors, Result};
use crate::options::IndexType::BPlusTree;
use crate::options::WriteBatchOptions;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::encoding::bool::encode;
use prost::{decode_length_delimiter, encode_length_delimiter, Message};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;

pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if self.option.index_type == BPlusTree && !self.seq_file_exist && !self.is_initial {
            return Err(Errors::CanNotUseWriteBatch);
        }
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}
impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKeyError);
        }
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecodType::NORMAL,
        };
        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKeyError);
        }
        let mut pending_writes = self.pending_writes.lock();
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }
        let mut record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecodType::DELETED,
        };
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum);
        }
        let lock_ = self.engine.batch_commit_lock.lock();
        let mut seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);
        let mut positions = HashMap::new();
        for (_, item) in pending_writes.iter() {
            let mut record = LogRecord {
                key: log_record_with_seq(item.key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };
            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.clone(), pos);
        }
        let mut finish_record = LogRecord {
            key: TXN_FIN_KEY.to_vec(),
            value: Default::default(),
            rec_type: LogRecodType::TXNFINSHED,
        };
        self.engine.append_log_record(&mut finish_record)?;
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        for (_, item) in pending_writes.iter() {
            if item.rec_type == LogRecodType::NORMAL {
                let record_pos = positions.get(&item.key).unwrap();
                self.engine.index.put(item.key.clone(), *record_pos);
            } else if item.rec_type == LogRecodType::DELETED {
                self.engine.index.delete(item.key.clone());
            }
        }
        pending_writes.clear();
        Ok(())
    }
}
pub(crate) fn log_record_with_seq(key: Vec<u8>, seq: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq, &mut enc_key).unwrap();

    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::Options;
    use crate::util;
    use std::path::PathBuf;
    #[test]
    fn test_write_batch1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("fail to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("fail to create write batch");
        let res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(res1.is_ok());
        let res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(res2.is_ok());
        let get_res1 = engine.get(util::rand_kv::get_test_key(1));
        //println!("{:?}", get_res1);
        assert_eq!(get_res1.err().unwrap(), Errors::KeyIsNotExist);

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let get_res2 = engine.get(util::rand_kv::get_test_key(1));
        //println!("{:?}", get_res2);

        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(seq_no, 1);
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("fail to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("fail to create write batch");
        let res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(res1.is_ok());
        let res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(res2.is_ok());

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        engine.close().expect("fail to close engine");
        let engine2 = Engine::open(opts.clone()).expect("fail to open engine");
        let list_key = engine2.list_keys();
        println!("{:#?}", list_key);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch3() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("fail to open engine");

        let mut wb_opts = WriteBatchOptions::default();
        wb_opts.max_batch_num = 1000000;
        let wb = engine
            .new_write_batch(wb_opts)
            .expect("fail to create write batch");
        for i in 0..100000 {
            let put_res = wb.put(
                util::rand_kv::get_test_key(i),
                util::rand_kv::get_test_value(10),
            );
            assert!(put_res.is_ok());
        }
        wb.commit();
    }
}
