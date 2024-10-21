use std::path::PathBuf;
#[derive(Clone, Debug)]
pub struct Options {
    pub dir_path: PathBuf,
    pub data_file_size: u64,
    pub sync_writes: bool,
    pub bytes_per_sync: usize,
    pub index_type: IndexType,
    pub mmap_at_startup: bool,
    pub data_file_merge_ratio:f32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexType {
    BTree,

    SkipList,

    BPlusTree,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("kv-data"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            data_file_merge_ratio: 0.5,
        }
    }
}

pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

pub struct WriteBatchOptions {
    pub max_batch_num: usize,
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum IOType {
    StandardIO,
    MemoryMap,
}
