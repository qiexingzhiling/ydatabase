mod file_io;
mod mmap;

use crate::errors::Result;
use crate::fio::file_io::FileIO;
use crate::fio::mmap::MMapIO;
use crate::options::IOType;
use std::error::Error;
use std::path::PathBuf;

pub trait IOManager: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;

    fn size(&self) -> u64;
}

pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Box<dyn IOManager> {
    match io_type {
        IOType::StandardIO => Box::new(FileIO::new(file_name).unwrap()),
        IOType::MemoryMap => Box::new(MMapIO::new(file_name).unwrap()),
    }
}
