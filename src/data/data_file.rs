use crate::data::log_record::{
    max_log_record_header_size, LogRecodPos, LogRecodType, LogRecord, ReadLogRecord,
};
use crate::errors::{Errors, Result};
use crate::fio;
use crate::fio::{new_io_manager, IOManager};
use crate::options::IOType;
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};
use std::ops::Index;
use std::path::PathBuf;
use std::sync::Arc;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const HINT_FILE_NAME: &str = "hint-index";
pub const MERGE_FINISH_FILE_NAME: &str = "merge-finished";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>,
    io_manager: Box<dyn fio::IOManager>,
}
impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32, io_type: IOType) -> Result<DataFile> {
        let file_name = get_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(file_name, io_type);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(HINT_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_merge_fin_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(MERGE_FINISH_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_seq_no_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(SEQ_NO_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }
    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn file_size(&self) -> u64 {
        self.io_manager.size()
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf).unwrap();
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn write_hint_record(&self, key: Vec<u8>, pos: LogRecodPos) -> Result<()> {
        let mut hint_record = LogRecord {
            key,
            value: pos.encode(),
            rec_type: LogRecodType::NORMAL,
        };
        let enc_record = hint_record.encode();
        self.write(&enc_record)?;
        Ok(())
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());

        self.io_manager.read(&mut header_buf, offset)?;
        let rec_type = header_buf.get_u8();
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        if key_size == 0 && value_size != 0 {
            return Err(Errors::ReadDataFileEOF);
        }
        let actual_header_size =
            1 + length_delimiter_len(key_size) + length_delimiter_len(value_size);
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;
        let mut log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),

            rec_type: LogRecodType::from_u8(rec_type),
        };

        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::WrongLogRecordCrc);
        }

        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }
    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) {
        self.io_manager = new_io_manager(get_data_file_name(dir_path, self.get_file_id()), io_type);
    }
}

pub fn get_data_file_name(path: PathBuf, file_id: u32) -> PathBuf {
    let v = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.to_path_buf().join(v)
}

mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 0, IOType::StandardIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let data_file_res2 = DataFile::new(dir_path.clone(), 0, IOType::StandardIO);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);

        let data_file_res3 = DataFile::new(dir_path.clone(), 660, IOType::StandardIO);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 660);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 100, IOType::StandardIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 100);

        let write_res1 = data_file1.write("aaa".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3 as usize);

        let write_res2 = data_file1.write("bbb".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 3 as usize);

        let write_res3 = data_file1.write("ccc".as_bytes());
        assert!(write_res3.is_ok());
        assert_eq!(write_res3.unwrap(), 3 as usize);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 200, IOType::StandardIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 200);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 700, IOType::StandardIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 700);

        let mut enc1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs-kv".as_bytes().to_vec(),
            rec_type: LogRecodType::NORMAL,
        };
        let write_res1 = data_file1.write(&enc1.encode());
        assert!(write_res1.is_ok());

        // 从起始位置读取
        let read_res1 = data_file1.read_log_record(0);
        assert!(read_res1.is_ok());
        let read_enc1 = read_res1.ok().unwrap().record;
        assert_eq!(enc1.key, read_enc1.key);
        assert_eq!(enc1.value, read_enc1.value);
        assert_eq!(enc1.rec_type, read_enc1.rec_type);

        // 从新的位置开启读取
        let mut enc2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "new-value".as_bytes().to_vec(),
            rec_type: LogRecodType::NORMAL,
        };
        let write_res2 = data_file1.write(&enc2.encode());
        assert!(write_res2.is_ok());

        let read_res2 = data_file1.read_log_record(24);
        assert!(read_res2.is_ok());
        let read_enc2 = read_res2.ok().unwrap().record;
        assert_eq!(enc2.key, read_enc2.key);
        assert_eq!(enc2.value, read_enc2.value);
        assert_eq!(enc2.rec_type, read_enc2.rec_type);

        // 类型是 Deleted
        let mut enc3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecodType::DELETED,
        };
        let write_res3 = data_file1.write(&enc3.encode());
        assert!(write_res3.is_ok());

        let read_res3 = data_file1.read_log_record(44);
        assert!(read_res3.is_ok());
        let read_enc3 = read_res3.ok().unwrap().record;
        assert_eq!(enc3.key, read_enc3.key);
        assert_eq!(enc3.value, read_enc3.value);
        assert_eq!(enc3.rec_type, read_enc3.rec_type);
    }
}
