use sled::Event;
use bincode::{DefaultOptions, Options};
use tokio;

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let db: sled::Db = sled::open("/tmp/testsled").unwrap();
    let tree = db.open_tree("test").unwrap();
    let binc = DefaultOptions::new().with_big_endian().with_fixint_encoding();
    let prefix = b"prefix/wat".to_vec();
    let fut = tree.watch_prefix(prefix.as_slice());

    let put_tree = tree.clone();
    let put_pfx = prefix.clone();
    tokio::spawn(async move {
        for i in 0..1000_u64 {
            let k = binc.serialize(&i).unwrap();
            let key = put_pfx.iter().cloned().chain(k.into_iter()).collect::<Vec<u8>>();
            let val = binc.serialize(&(i as u128 + 4242424242 * 1717)).unwrap();
            put_tree.insert(key, val).unwrap();
        }
       put_tree.flush().unwrap();
    });

    let mut i: usize = 100;
    tokio::pin!(fut);
    loop {
      if let Some(Event::Insert{ key, value }) = fut.as_mut().await {
        let k: u64 = binc.deserialize(&key[prefix.len()..]).unwrap();
        let v: u128 = binc.deserialize(&value).unwrap();
        println!("{} - {:?} - {:?}", i, k, v);
        i += 1;
      }
    }
}

