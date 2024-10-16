use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};
use std::process::Output;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum LogRecodType {
    NORMAL = 1,
    DELETED = 2,
    TXNFINSHED=3,
}

impl LogRecodType {
    pub fn from_u8(val: u8) -> LogRecodType {
        match val {
            1 => LogRecodType::NORMAL,
            2 => LogRecodType::DELETED,
            3 => LogRecodType::TXNFINSHED,
            _ => panic!("wrong LogRecodType"),
        }
    }
}

#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecodType,
}
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

#[derive(Debug, Copy, Clone)]
pub struct LogRecodPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecodPos,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }
    pub fn get_crc(&mut self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }

    pub fn encode_and_get_crc(&mut self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());
        buf.put_u8(self.rec_type as u8);

        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);
        (buf.to_vec(), crc)
    }
    pub fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_log_record_encode() {
        let mut rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecodType::NORMAL,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(1020360578, rec1.get_crc());

        let mut rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecodType::NORMAL,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(3756865478, rec2.get_crc());

        let mut rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecodType::DELETED,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(1867197446, rec3.get_crc());
    }
}
