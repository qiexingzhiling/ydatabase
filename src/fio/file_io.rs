use crate::errors::Errors;
use crate::errors::Result;
use crate::fio::IOManager;
use log::error;
use parking_lot::{RwLock, RwLockReadGuard};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(filename: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(filename)
        {
            Ok(fd) => {
                return Ok(FileIO {
                    fd: Arc::new(RwLock::new(fd)),
                });
            }
            Err(e) => {
                error!("fail to open file:{}", e);
                return Err(Errors::FileOpenError);
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file err:{}", e);
                return Err(Errors::FileReadError);
            }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write to data file err:{}", e);
                return Err(Errors::FileWriteError);
            }
        }
    }

    fn sync(&self) -> crate::errors::Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("sync to data file err:{}", e);
            return Err(Errors::FileSyncError);
        } else {
            return Ok(());
        }
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        let metadata = read_guard.metadata().unwrap();
        metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    #[test]
    fn file_io_write_test() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());
        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());
        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn file_io_read_test() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());
        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let mut buf = [0u8; 5];
        let read_res1 = fio.read(&mut buf, 0);
        assert!(read_res1.is_ok());
        assert_eq!(5, read_res1.ok().unwrap());
        let mut buf2 = [0u8; 5];
        let res2 = fio.read(&mut buf2, 5);
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());
        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn file_io_sync_test() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());
        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let res_sync = fio.sync();
        assert!(res_sync.is_ok());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }
}
