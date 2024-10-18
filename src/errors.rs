use std::result;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Errors {
    #[error("fail to read from file")]
    FileReadError,
    #[error("fail to write bytes to file")]
    FileWriteError,
    #[error("fail to sync file")]
    FileSyncError,
    #[error("fail to open file")]
    FileOpenError,
    #[error("key is empty")]
    KeyIsEmpty,
    #[error("memory index failed to update")]
    IndexUpdateFailed,
    #[error("key is not exist")]
    KeyIsNotExist,
    #[error("data file does not exist")]
    DataFileNotFound,
    #[error("directory does not exist")]
    DirIsNotExist,
    #[error("data file is empty")]
    DataFileIsEmpty,
    #[error("fail to create database directory")]
    FailToCreateDatabaseDir,
    #[error("fail to read database directory")]
    FailToReadDatabasedir,
    #[error("data directory corrupped")]
    DataDirectoryCorruped,
    #[error("read data file EOF")]
    ReadDataFileEOF,
    #[error("Wrong Log Rrcord Crc")]
    WrongLogRecordCrc,
    #[error("Empty Key Error")]
    EmptyKeyError,
    #[error("Invalid Key Error")]
    ExceedMaxBatchNum,
    #[error("Merging is progressing,please try again later")]
    MergingIsProgressing,
    #[error("can not use write batch")]
    CanNotUseWriteBatch,
}
pub type Result<T> = result::Result<T, Errors>;
