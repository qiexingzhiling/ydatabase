use bytes::Bytes;

pub fn get_test_key(i: i32) -> Bytes {
    Bytes::from(std::format!("bitcask-key-{:09}", i))
}

pub fn get_test_value(i: i32) -> Bytes {
    Bytes::from(std::format!("bitcask-value-{:09}", i))
}

#[cfg(test)]
mod tests {
    use crate::util::rand_kv::get_test_key;
    #[test]
    fn test_get_test_key_and_value() {
        for i in 0..=10 {
            assert!(get_test_key(i).len() > 0);
        }

        for i in 0..=10 {
            assert!(get_test_key(i).len() > 0);
        }
    }
}
