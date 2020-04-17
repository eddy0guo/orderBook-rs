mod consume;
mod models;
mod util;

extern crate kafka;
extern crate env_logger;
extern crate rustc_serialize;


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
use std::sync::mpsc::channel;

static mut available_buy_orders: Vec<models::EngineOrder> = Vec::new();
static mut available_sell_orders: Vec<models::EngineOrder> = Vec::new();
static mut trades: Vec<models::TradeInfo> = Vec::new();
static mut market_id: String = String::new();

lazy_static! {
    // let orderTopic = "orderStream".to_owned();
    // let bridgeTopic = "bridgeStream".to_owned();

    static ref CLIENTDB: Mutex<postgres::Client> = Mutex::new({
        println!("lazy_static--postgres");
        connetDB().unwrap()
    });

    static ref ORDER_STREAM: Mutex<Consumer> = Mutex::new({
        println!("lazy_static--ORDER_STREAM");
        unsafe{
                consumer_init(market_id.clone()).unwrap()
        }
    });

     static ref BRIDGE_STREAM: Mutex<Consumer> = Mutex::new({
         println!("lazy_static-BRIDGE_STREAM-");
         unsafe{
                 consumer_init(market_id.clone()).unwrap()
         }
        //consumer_init("bridgeStream".to_owned()).unwrap()
    });

     /*
     static ref ORDERS: Mutex<Vec<models::EngineOrder>> = Mutex::new({
        models::list_available_orders()
        //consumer_init("bridgeStream".to_owned()).unwrap()
    });
    */
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
    if let Ok(client) = consumer_init(topic) {
        *crate::ORDER_STREAM.lock().unwrap() = client;
        return true;
    }
    false
}

fn connetDB() -> Option<postgres::Client> {
    let mut client;
    let mut dbname = "product".to_string();
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

fn init(market: & str) {
    unsafe {
        available_buy_orders = models::list_available_orders("buy", market);
        available_sell_orders = models::list_available_orders("sell", market);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("main--");
    env_logger::init();
    for argument in env::args() {
        if argument.contains("--market_id=") {
            let market_option:Vec<&str> = argument.as_str().split('=').collect();
            init(market_option[1].clone());
            unsafe {
                market_id = market_option[1].to_string();
            }
            println!("You passed --help as one of the arguments!");
        }
    }
    // init("ASIM-CNYC");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task1 = async {
        consume::engine_start();
    };
    rt.spawn(task1);

    let task2 = async {
        //consume::flush_start();
        println!("ctrl-c received22!");
    };
    rt.spawn(task2);
    tokio::signal::ctrl_c().await?;
    println!("ctrl-c received!");
    Ok(())
}
