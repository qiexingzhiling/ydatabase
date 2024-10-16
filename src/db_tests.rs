use bytes::Bytes;
use std::path::PathBuf;

use crate::{
    db::Engine,
    errors::Errors,
    options::Options,
    util::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_close() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-close");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    let res1 = engine.put(get_test_key(222), get_test_value(222));
    assert!(res1.is_ok());

    let close_res = engine.close();
    assert!(close_res.is_ok());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}
#[test]
fn test_engine_sync() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-sync");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    let res1 = engine.put(get_test_key(222), get_test_value(222));
    assert!(res1.is_ok());

    let close_res = engine.sync();
    assert!(close_res.is_ok());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}