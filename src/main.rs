mod consume;
mod models;
mod util;

extern crate env_logger;
extern crate kafka;
extern crate rustc_serialize;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;
use std::io::BufReader;

#[macro_use]
use postgres::{Client, NoTls};
use std::env;
use std::fs::OpenOptions;
use std::sync::Mutex;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;
use log::Level;
use log::LevelFilter;

use crate::models::get_max_transaction_id;
use crate::util::get_current_time;
use chrono::prelude::*;
use kafka::producer::{Producer, Record, RequiredAcks};
use std::fmt::Write;
use std::ptr::null;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::time::Instant;

static mut available_buy_orders: Vec<models::EngineOrder> = Vec::new();
static mut available_sell_orders: Vec<models::EngineOrder> = Vec::new();
static mut trades: Vec<consume::engine::EngineTrade> = Vec::new();
static mut market_id: String = String::new();
static kafka_server: &str = "localhost:9092";

const READ_ORDER_TABLE: &str = "mist_orders2";
const WRITE_ORDER_TABLE: &str = "mist_orders2";
const WRITE_ORDER_TMP_TABLE: &str = "mist_orders_tmp2";

const READ_TRADE_TABLE: &str = "mist_trades_tmp2";
const WRITE_TRADE_TABLE: &str = "mist_trades2";
const WRITE_TRADE_TMP_TABLE: &str = "mist_trades_tmp2";

lazy_static! {
    static ref CLIENTDB: Mutex<postgres::Client> = Mutex::new({
        info!("lazy_static--postgres");
        connetDB().unwrap()
    });
    static ref ORDER_CONSUMER: Mutex<Consumer> = Mutex::new({
        info!("lazy_static--ORDER_CONSUMER");
        unsafe { consumer_init(market_id.clone()).unwrap() }
    });
    static ref TRADE_CONSUMER: Mutex<Consumer> = Mutex::new({
        info!("lazy_static-CONSUMER-");
        unsafe { consumer_init(market_id.clone()).unwrap() }
    });
    static ref TRADE_PRODUCER: Mutex<Producer> = Mutex::new({
        info!("lazy_static-TRADE_PRODUCER-");
        producer_init().unwrap()
    });
}

pub fn restartDB() -> bool {
    let now = Local::now();
    info!("restart postgresql {:?}", now);
    // let client =  connetDB();
    if let Some(client) = connetDB() {
        *crate::CLIENTDB.lock().unwrap() = client;
        return true;
    }
    false
}

pub fn restart_kafka(topic: String) -> bool {
    let now = Local::now();
    info!("reconnect {} kafka stream  at {:?}", topic, now);
    if let Ok(client) = consumer_init(topic) {
        *crate::ORDER_CONSUMER.lock().unwrap() = client;
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
        info!("have no MIST_MODE env");
    }

    let url = format!("host=pgm-wz9m1yb4h5g4sl7x127770.pg.rds.aliyuncs.com port=1433 user=product password=myHzSesQc7TXSS5HOXZDsgq7SNUHY2 dbname={}", dbname);

    if let Ok(tmp) = Client::connect(&url, NoTls) {
        client = tmp;
    } else {
        info!("connect postgresql failed");
        return None;
    }
    Some(client)
}

fn consumer_init(topic: String) -> Result<Consumer, KafkaError> {
    //fn  order_consumer_init() -> Result<(), KafkaError> {
    let brokers = vec![kafka_server.to_owned()];
    let group = "mist".to_owned();
    let con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;
    Ok(con)
}

fn producer_init() -> Result<Producer, KafkaError> {
    let mut producer = Producer::from_hosts(vec![kafka_server.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();
    Ok(producer)
}

fn init(market: &str) {
    unsafe {
        info!("start loading data at {}", get_current_time());
        available_buy_orders = models::list_available_orders("buy", market);
        info!(
            "finished loading {} buy data at {}",
            available_buy_orders.len(),
            get_current_time()
        );
        available_sell_orders = models::list_available_orders("sell", market);
        info!(
            "finished loading {} sell data at {}",
            available_sell_orders.len(),
            get_current_time()
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // env_logger::init();
    env_logger::builder()
        .format_timestamp(Option::from(env_logger::TimestampPrecision::Millis))
        // .filter_level(LevelFilter::)
        .init();
    for argument in env::args() {
        if argument.contains("--market-id=") {
            let market_option: Vec<&str> = argument.as_str().split('=').collect();
            init(market_option[1].clone());
            unsafe {
                market_id = market_option[1].to_string();
            }
            info!("You passed --market_id as one of the arguments!");
        }
    }
    // init("ASIM-CNYC");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task1 = async {
        consume::engine_start();
    };
    rt.spawn(task1);

    let task2 = async {
        consume::flush_start();
    };
    rt.spawn(task2);

    tokio::signal::ctrl_c().await?;
    info!("ctrl-c received!");
    Ok(())
}
