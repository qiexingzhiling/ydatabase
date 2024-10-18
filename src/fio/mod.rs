mod file_io;

use crate::errors::Result;
use crate::fio::file_io::FileIO;
use std::error::Error;
use std::path::PathBuf;

pub trait IOManager: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;

    fn size(&self) -> u64;
}

pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
