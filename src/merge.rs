use crate::batch::{log_record_with_seq, parse_log_record_key, NON_TRANSACTION_SEQ_NO};
use crate::data::data_file::{
    get_data_file_name, DataFile, HINT_FILE_NAME, MERGE_FINISH_FILE_NAME, SEQ_NO_FILE_NAME,
};
use crate::data::log_record::{decode_log_record_pos, LogRecodType, LogRecord};
use crate::db::Engine;
use crate::errors::{Errors, Result};
use crate::options::Options;
use log::error;
use std::fs;
use std::path::PathBuf;

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge-finshed".as_bytes();
impl Engine {
    pub fn merge(&mut self) -> Result<()> {
        let lock = self.merging_lock.try_lock();

        if lock.is_none() {
            return Err(Errors::MergingIsProgressing);
        }

        let merge_files = self.rotate_merge_file()?;
        let merge_path = get_merge_path(self.option.dir_path.clone());
        if merge_path.is_dir() {
            fs::remove_dir_all(merge_path.clone()).unwrap();
        }
        if let Err(e) = fs::create_dir_all(merge_path.clone()) {
            error!("Failed to create merge directory {}", e);
            return Err(Errors::FailToCreateDatabaseDir);
        }
        let merge_files = self.rotate_merge_file()?;

        let mut merge_db_opts = Options::default();
        merge_db_opts.dir_path = merge_path.clone();
        merge_db_opts.data_file_size = self.option.data_file_size;

        let mut merge_db = Engine::open(merge_db_opts)?;
        let hint_file = DataFile::new_hint_file(merge_path.clone())?;
        for data_file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (mut log_record, size) = match data_file.read_log_record(offset) {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };
                let (real_key, _) = parse_log_record_key(log_record.key.clone());
                if let Some(index_pos) = self.index.get(real_key.clone()) {
                    if index_pos.file_id == data_file.get_file_id() && index_pos.offset == offset {
                        log_record.key =
                            log_record_with_seq(real_key.clone(), NON_TRANSACTION_SEQ_NO);
                        let log_record_pos = merge_db.append_log_record(&mut log_record)?;
                        hint_file.write_hint_record(real_key.clone(), log_record_pos)?;
                    }
                }
                offset += size as u64;
            }
        }

        merge_db.sync()?;
        hint_file.sync()?;

        let non_merge_files_id = merge_files.last().unwrap().get_file_id() + 1;
        let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
        let mut merge_fin_record = LogRecord {
            key: MERGE_FIN_KEY.to_vec(),
            value: non_merge_files_id.to_string().into_bytes(),
            rec_type: LogRecodType::NORMAL,
        };
        let enc_record = merge_fin_record.encode();
        merge_fin_file.write(&enc_record)?;
        merge_fin_file.sync()?;

        Ok(())
    }

    fn rotate_merge_file(&self) -> Result<Vec<DataFile>> {
        let mut merge_file_ids = Vec::new();
        let mut older_files = self.older_file.write();
        for fid in older_files.keys() {
            merge_file_ids.push(*fid);
        }
        let mut active_file = self.active_file.write();
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        let new_file = DataFile::new(self.option.dir_path.clone(), active_file_id + 1)?;
        *active_file = new_file;

        let old_file = DataFile::new(self.option.dir_path.clone(), active_file_id)?;
        older_files.insert(active_file_id, old_file);
        merge_file_ids.push(active_file_id);
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for file_id in merge_file_ids.iter() {
            let data_file = DataFile::new(self.option.dir_path.clone(), *file_id)?;
            merge_files.push(data_file);
        }
        Ok(merge_files)
    }

    pub(crate) fn load_index_from_hint_files(&self) -> Result<()> {
        let hint_file_name = self.option.dir_path.join(HINT_FILE_NAME);
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = DataFile::new_hint_file(self.option.dir_path.clone())?;
        let mut offset = 0;

        loop {
            let (log_record, size) = match hint_file.read_log_record(offset) {
                Ok(result) => (result.record, result.size),
                Err(e) => {
                    if e == Errors::ReadDataFileEOF {
                        break;
                    }
                    return Err(e);
                }
            };

            let log_record_pos = decode_log_record_pos(log_record.value);
            self.index.put(log_record.key, log_record_pos);
            offset += size as u64;
        }
        Ok(())
    }
}

fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = std::format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}

pub(crate) fn load_merge_files(dir_path: PathBuf) -> Result<()> {
    let mut merge_path = get_merge_path(dir_path.clone());
    if !merge_path.is_dir() {
        return Ok(());
    }
    let dir = match fs::read_dir(merge_path.clone()) {
        Ok(dir) => dir,
        Err(e) => {
            error!("Failed to read merge directory {}", e);
            return Err(Errors::FailToReadDatabasedir);
        }
    };
    let mut merge_file_names = Vec::new();
    let mut merge_finshed = false;
    for file in dir {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            if file_name.ends_with(MERGE_FINISH_FILE_NAME) {
                merge_finshed = true;
            }
            if file_name.ends_with(SEQ_NO_FILE_NAME) {
                continue;
            }
            merge_file_names.push(entry.file_name());
        }
    }
    if !merge_finshed {
        fs::remove_dir_all(merge_path.clone()).unwrap();
        return Ok(());
    }

    let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
    let merge_fin_record = merge_fin_file.read_log_record(0)?;
    let v = String::from_utf8(merge_fin_record.record.value).unwrap();
    let non_merge_fid = v.parse::<u32>().unwrap();

    for file_id in 0..non_merge_fid {
        let file = get_data_file_name(dir_path.clone(), file_id);
        if file.is_file() {}
        fs::remove_file(file).unwrap();
    }

    for file_name in merge_file_names {
        let src_path = merge_path.join(file_name.clone());
        let dest_path = dir_path.join(file_name.clone());
        fs::rename(src_path, dest_path).unwrap();
    }

    fs::remove_dir_all(merge_path).unwrap();

    Ok(())
}
