use bytes::Bytes;
use kv_data::db;
use kv_data::options::{IndexType, Options};
use std::fs::OpenOptions;
fn main() {
    let opt = Options::default();
    let engine = db::Engine::open(opt).expect("Fail to open db");

    let res1 = engine.put(Bytes::from("key2"), Bytes::from("value2"));
    assert!(res1.is_ok());

    let res2 = engine.get(Bytes::from("key2"));
    assert!(res2.is_ok());

    let val = res2.unwrap();
    println!("{:?}", String::from_utf8(val.to_vec()));

    let res3 = engine.delete(Bytes::from("key2"));
    assert!(res3.is_ok());
}
