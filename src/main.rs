mod consume;
mod models;

extern crate kafka;
extern crate env_logger;

use std::fmt::Write;
use std::io::BufReader;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;

#[macro_use]
use postgres::{Client, NoTls};
use std::env;
use std::fs::OpenOptions;
use std::sync::Mutex;

#[macro_use]
extern crate lazy_static;

use std::time::Instant;
use chrono::prelude::*;
use std::ptr::null;

lazy_static! {
    // let orderTopic = "orderStream".to_owned();
    // let bridgeTopic = "bridgeStream".to_owned();

    static ref CLIENTDB: Mutex<postgres::Client> = Mutex::new({
        connetDB().unwrap()
    });

    static ref ORDER_STREAM: Mutex<Consumer> = Mutex::new({
        consumer_init("orderStream".to_owned()).unwrap()
    });

     static ref BRIDGE_STREAM: Mutex<Consumer> = Mutex::new({
        consumer_init("orderStream".to_owned()).unwrap()
        //consumer_init("bridgeStream".to_owned()).unwrap()
    });
}

pub fn restartDB() -> bool {
    let now = Local::now();
    println!("restart postgresql {:?}", now);
    // let client =  connetDB();
    if let Some(client) = connetDB() {
        *crate::CLIENTDB.lock().unwrap() = client;
        return true;
    }
    false
}

pub fn restart_kafka(topic: String) -> bool {
    let now = Local::now();
    println!("reconnect {} kafka stream  at {:?}", topic, now);
    // let client =  connetDB();
    if topic == "bridge".to_owned() {
        if let Ok(client) = consumer_init(topic) {
            *crate::BRIDGE_STREAM.lock().unwrap() = client;
            return true;
        }
        false
    } else {
        if let Ok(client) = consumer_init(topic) {
            *crate::ORDER_STREAM.lock().unwrap() = client;
            return true;
        }
        false
    }
}

fn connetDB() -> Option<postgres::Client> {
    let mut client;
    let mut dbname = "dev".to_string();
    if let Some(mist_mode) = env::var_os("MIST_MODE") {
        dbname = mist_mode.into_string().unwrap();
    } else {
        println!("have no MIST_MODE env");
    }

    let url = format!("host=pgm-wz9m1yb4h5g4sl7x127770.pg.rds.aliyuncs.com port=1433 user=product password=myHzSesQc7TXSS5HOXZDsgq7SNUHY2 dbname={}", dbname);

    if let Ok(tmp) = Client::connect(&url, NoTls) {
        client = tmp;
    } else {
        println!("connect postgresql failed");
        return None;
    }
    Some(client)
}

fn consumer_init(topic: String) -> Result<Consumer, KafkaError> {
//fn  order_consumer_init() -> Result<(), KafkaError> {
    let brokers = vec!["localhost:9092".to_owned()];
    let group = "mist".to_owned();
    let con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;
    Ok(con)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task1 = async {
        consume::order_start();
    };
    rt.spawn(task1);

    let task2 = async {
        consume::bridge_start();
    };
    rt.spawn(task2);

    tokio::signal::ctrl_c().await?;
    println!("ctrl-c received!");
    Ok(())
}
