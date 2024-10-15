pub enum LogRecodType {
    NORMAL=1,
    DELETED=2,
}

pub struct LogRecord {
    pub(crate) key:Vec<u8>,
    pub (crate) value:Vec<u8>,
    pub (crate) rec_type:LogRecodType,
}


#[derive(Debug, Copy, Clone)]
pub struct LogRecodPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}