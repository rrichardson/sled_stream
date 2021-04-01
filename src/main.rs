use sled::{ Db, Event };
use bincode::{DefaultOptions, Options};
use tokio;

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let db: sled::Db = sled::open("/tmp/testsled").unwrap();
    test_future(db.clone()).await.unwrap();
    test_iterator(db).await
}


async fn test_future(db: Db) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let tree = db.open_tree("future").unwrap();
    let binc = DefaultOptions::new().with_big_endian().with_fixint_encoding();
    let prefix = b"wat/".to_vec();
    let sub = tree.watch_prefix(prefix.as_slice());

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

    println!("Testing future");
    let mut i: usize = 0;
    tokio::pin!(sub);
    loop {
      if let Some(Event::Insert{ key, value }) = sub.as_mut().await {
        let k: u64 = binc.deserialize(&key[prefix.len()..]).unwrap();
        let _v: u128 = binc.deserialize(&value).unwrap();
        //println!("future: {} - {:?} - {:?}", i, k, v);
        i += 1;
        if k == 999 {
            break;
        }
      }
    }
    if i < 999 {
        println!("Future missed {} records D: ", 999 - i )
    } else {
        println!("Future's all good :)");
    }
    Ok(())
}

async fn test_iterator(db: Db) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let tree = db.open_tree("iter").unwrap();
    let binc = DefaultOptions::new().with_big_endian().with_fixint_encoding();
    let prefix = b"hmmm/".to_vec();
    let mut sub = tree.watch_prefix(prefix.as_slice());

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

    println!("Testing Iterator");
    let mut i: usize = 0;
    while let Some(Event::Insert{ key, value }) = sub.next() {
        let k: u64 = binc.deserialize(&key[prefix.len()..]).unwrap();
        let _v: u128 = binc.deserialize(&value).unwrap();
        //println!("iterator: {} - {:?} - {:?}", i, k, v);
        i += 1;
        if k == 999 {
            break;
        }
    }
    if i < 999 {
        println!("Iterator Missed {} records D: ", 999 - i )
    } else {
        println!("Iterator's all good :)");
    }
    Ok(())
}


