use crate::batch::{log_record_with_seq, parse_log_record_key, NON_TRANSACTION_SEQ_NO};
use crate::data::data_file::{DataFile, DATA_FILE_NAME_SUFFIX, MERGE_FINISH_FILE_NAME};
use crate::data::log_record::LogRecodType::DELETED;
use crate::data::log_record::{LogRecodPos, LogRecodType, LogRecord, TransactionRecord};
use crate::errors::{Errors, Result};
use crate::merge::load_merge_files;
use crate::options::IOType::{MemoryMap, StandardIO};
use crate::options::{IOType, IndexType, Options};
use crate::{index, options};
use bytes::Bytes;
use fs2::FileExt;
use jammdb::Data;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) const FILE_LOCK_NAME: &str = "flock";
pub const SEQ_NO_KEY: &str = "seq.no";

pub struct Engine {
    pub(crate) option: Arc<Options>,
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    pub(crate) older_file: Arc<RwLock<HashMap<u32, DataFile>>>,
    pub(crate) index: Box<dyn index::Indexer>,
    pub(crate) file_ids: Vec<u32>,
    pub(crate) batch_commit_lock: Mutex<()>,
    pub(crate) seq_no: Arc<AtomicUsize>,
    pub(crate) merging_lock: Mutex<()>,
    pub(crate) seq_file_exist: bool,
    pub(crate) is_initial: bool,
    pub(crate) lock_file: File,
    pub(crate) bytes_write: Arc<AtomicUsize>,
    pub(crate) reclaim_size: Arc<AtomicUsize>,
}

pub struct Stat {
    key_num: usize,
    data_file_num: usize,
    pub(crate) reclaim_size: usize,
    disk_size:u64,
}
const INITIAL_FILE_ID: u32 = 0;

impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }

        let mut is_initial = false;
        let options = opts.clone();
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            is_initial = true;
            if let Err(e) = std::fs::create_dir_all(&dir_path) {
                warn!("create database directory err:{}", e);
                return Err(Errors::FailToCreateDatabaseDir);
            }
        }

        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(dir_path.join(FILE_LOCK_NAME))
            .unwrap();
        if let Err(e) = lock_file.try_lock_exclusive() {
            return Err(Errors::DataBaseIsUsing);
        }

        let entries = fs::read_dir(dir_path.clone()).unwrap();
        if entries.count() == 0 {
            is_initial = true;
        }

        load_merge_files(dir_path.clone())?;

        let mut data_files = load_data_files(dir_path.clone(), options.mmap_at_startup)?;
        let mut file_ids: Vec<u32> = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        let mut older_files = HashMap::new();
        if data_files.len() > 2 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID, IOType::StandardIO)?,
        };
        let mut engine = Self {
            option: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_file: Arc::new(RwLock::new(older_files)),
            index: index::new_indexer(options.index_type, options.dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
            merging_lock: Mutex::new(()),
            seq_file_exist: false,
            is_initial,
            lock_file,
            bytes_write: Arc::new(AtomicUsize::new(0)),
            reclaim_size:Arc::new(AtomicUsize::new(0)),
        };

        if engine.option.index_type == IndexType::BPlusTree {
            engine.load_index_from_data_files()?;

            let current_seq_no = engine.load_index_from_data_files()?;
            if current_seq_no > 0 {
                engine.seq_no.store(current_seq_no, Ordering::SeqCst);
            }

            if engine.option.mmap_at_startup {
                engine.reset_io_type();
            }
        }

        if IndexType::BPlusTree == engine.option.index_type {
            let (exist, seqno) = engine.load_seq_no();
            engine.seq_no.store(seqno, Ordering::SeqCst);
            engine.seq_file_exist = exist;

            let active_file = engine.active_file.write();
            active_file.set_write_off(active_file.file_size());
        }

        Ok(engine)
    }

    pub fn close(&self) -> Result<()> {
        if !self.option.dir_path.is_dir() {
            return Ok(());
        }
        let seq_no_file = DataFile::new_seq_no_file(self.option.dir_path.clone())?;
        let seq_no = self.seq_no.load(Ordering::SeqCst);
        let mut record = LogRecord {
            key: SEQ_NO_KEY.as_bytes().to_vec(),
            value: seq_no.to_string().into_bytes(),
            rec_type: LogRecodType::NORMAL,
        };

        seq_no_file.write(&record.encode())?;
        seq_no_file.sync()?;

        let read_guard = self.active_file.read();
        read_guard.sync()?;
        self.lock_file.unlock().unwrap();

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    pub fn stat(&mut self)->Result<Stat> {
        let keys=self.list_keys().unwrap();
        let older_files=self.older_file.read();
        Ok(
            Stat{
                key_num: keys.len(),
                data_file_num: keys.len()+1,
                reclaim_size: self.reclaim_size.load(Ordering::SeqCst),
                disk_size: 0,
            }
        )
    }
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut record: LogRecord = LogRecord {
            key: log_record_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: value.to_vec(),
            rec_type: LogRecodType::NORMAL,
        };

        let log_record_pos = self.append_log_record(&mut record)?;

        if let Some(old_pos)=self.index.put(key.to_vec(),log_record_pos) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        
        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        let mut record = LogRecord {
            key: log_record_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: Default::default(),
            rec_type: LogRecodType::DELETED,
        };
        self.append_log_record(&mut record)?;
        //let ok = self.index.delete(key.to_vec());
        let pos=self.index.get(key.to_vec()).unwrap();
        self.reclaim_size.fetch_add(pos.size as usize, Ordering::SeqCst);
        
        
        if let Some(old_pos)=self.index.delete(key.to_vec()) {
            self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyIsNotExist);
        }
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_file = self.older_file.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = older_file.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };
        if log_record.rec_type == LogRecodType::DELETED {
            return Err(Errors::KeyIsNotExist);
        }
        Ok(log_record.value.into())
    }

    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecodPos) -> Result<Bytes> {
        let active_file = self.active_file.read();
        let older_file = self.older_file.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = older_file.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };
        if log_record.rec_type == LogRecodType::DELETED {
            return Err(Errors::KeyIsNotExist);
        }
        Ok(log_record.value.into())
    }

    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecodPos> {
        let dir_path = self.option.dir_path.clone();
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        let mut active_file = self.active_file.write();
        if active_file.get_write_off() + record_len > self.option.data_file_size {
            active_file.sync()?;
            let current_id = active_file.get_file_id();
            let mut older_file = self.older_file.write();
            let old_file = DataFile::new(dir_path.clone(), current_id, IOType::StandardIO)?;
            older_file.insert(current_id, old_file);
            let new_file = DataFile::new(dir_path.clone(), current_id + 1, IOType::StandardIO)?;
            *active_file = new_file;
        }
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        let previous = self
            .bytes_write
            .fetch_add(enc_record.len(), Ordering::SeqCst);
        let mut need_sync = self.option.sync_writes;
        if !need_sync
            && self.option.bytes_per_sync > 0
            && previous + enc_record.len() >= self.option.bytes_per_sync
        {
            need_sync = true;
        }
        if need_sync {
            active_file.sync()?;
            self.bytes_write.store(0, Ordering::SeqCst);
        }
        Ok(LogRecodPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
            size:enc_record.len() as u32,
        })
    }
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        let mut current_seq_no: usize = NON_TRANSACTION_SEQ_NO;

        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        let mut has_merged = false;
        let mut non_merged_fid = 0;
        let merge_fin_file = self.option.dir_path.join(MERGE_FINISH_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_fin_file = DataFile::new_merge_fin_file(self.option.dir_path.clone())?;
            let merge_fin_record = merge_fin_file.read_log_record(0)?;
            let v = String::from_utf8(merge_fin_record.record.value).unwrap();
            non_merged_fid = v.parse::<u32>().unwrap();
            has_merged = true;
        }

        let mut transaction_records = HashMap::new();

        let active_file = self.active_file.read();
        let older_file = self.older_file.read();
        for (i, file_id) in self.file_ids.iter().enumerate() {
            if has_merged && *file_id < non_merged_fid {
                continue;
            }
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_file.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };

                let log_record_pos = LogRecodPos {
                    file_id: *file_id,
                    offset,
                    size: size as u32,
                };

                let (real_key, seq_no) = parse_log_record_key(log_record.key.clone());
                // 非事务提交的情况，直接更新内存索引
                if seq_no == NON_TRANSACTION_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos);
                } else {
                    // 事务有提交的标识，更新内存索引
                    if log_record.rec_type == LogRecodType::TXNFINSHED {
                        let records: &Vec<TransactionRecord> =
                            transaction_records.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.rec_type,
                                txn_record.pos,
                            );
                        }
                        transaction_records.remove(&seq_no);
                    } else {
                        log_record.key = real_key;
                        transaction_records
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
                }
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }
                offset += size as u64;
            }
            if i == self.file_ids.len() {
                active_file.set_write_off(offset);
            }
        }
        Ok(current_seq_no)
    }
    fn update_index(&self, key: Vec<u8>, log_recod_type: LogRecodType, pos: LogRecodPos) {
        if log_recod_type == LogRecodType::NORMAL {
            if let Some(old_pos)=self.index.put(key.clone(), pos){
                self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
            }
        }
        if log_recod_type == LogRecodType::DELETED {
            let mut size=pos.size;
            if let Some(old_pos)=self.index.delete(key) {
                size+=old_pos.size;
            }
            self.reclaim_size.fetch_add(size as usize,Ordering::SeqCst);
        }
    }

    fn load_seq_no(&self) -> (bool, usize) {
        let file_name = self.option.dir_path.join("SEQ_NO_FILE_NAME");
        if !file_name.is_file() {
            return (false, 0);
        }

        let seq_no_file = DataFile::new_seq_no_file(self.option.dir_path.clone()).unwrap();
        let record = match seq_no_file.read_log_record(0) {
            Ok(result) => result.record,
            Err(e) => panic!("fail to read seq no {}", e),
        };

        let v = String::from_utf8(record.value).unwrap();
        let seq_no = v.parse::<usize>().unwrap();

        fs::remove_file(file_name).unwrap();

        (true, seq_no)
    }

    fn reset_io_type(&self) {
        let mut active_file = self.active_file.write();
        active_file.set_io_manager(self.option.dir_path.clone(), IOType::StandardIO);

        let mut older_file = self.older_file.write();
        for (_, file) in older_file.iter_mut() {
            file.set_io_manager(self.option.dir_path.clone(), IOType::StandardIO);
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("error closing engine thread: {}", e);
        }
    }
}

fn load_data_files(dir_path: PathBuf, use_map: bool) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailToReadDatabasedir);
    }

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Errors::DataDirectoryCorruped);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }
    if file_ids.is_empty() {
        return Ok(data_files);
    }
    file_ids.sort();
    for file_id in file_ids.iter() {
        let mut io_type = StandardIO;
        if use_map {
            io_type = MemoryMap;
        }
        let data_file = DataFile::new(dir_path.clone(), *file_id, io_type)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}
fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirIsNotExist);
    }
    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileIsEmpty);
    }

    if opts.data_file_merge_ratio<0 as f32 || opts.data_file_merge_ratio>1 as f32 {
        return Some(Errors::DataFileMergeRatioIsInvalid);
    }

    None
}
