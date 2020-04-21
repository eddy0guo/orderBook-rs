mod flush;
pub(crate) mod engine;
extern crate kafka;

use std::cmp::Ord;
use std::collections::BTreeMap;

use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde::Deserialize;
use rustc_serialize::json;
use super::models::*;
use super::util::*;
use std::env;
use std::ops::Mul;
use std::any::Any;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;
use crate::trades;
use chrono::prelude::*;
use chrono::offset::LocalResult;


#[derive(Deserialize, Debug)]
struct Transfer {
    private_key: String,
    fromaccount: String,
    toaccount: String,
    amount: f64,
    token: String,
}

#[derive(Deserialize, Debug)]
struct orderFilter {
    id: String,
}

#[derive(Deserialize, Debug)]
struct MistInfo {
    active_address: i32,
    volume: f64,
    trades: i32,
}

pub fn engine_start() {
    loop {
        let mut mss = crate::ORDER_CONSUMER.lock().unwrap().poll().unwrap();
        if mss.is_empty() {
            // println!("No order messages available right now.");
            continue;
        }
        for ms in mss.iter() {
            for m in ms.messages() {
                println!("matched ----555");
                let mut message = String::new();
                let message = String::from_utf8_lossy(m.value);
                let mut decoded_message: EngineOrder = Default::default();
                if let Ok(tmp) = json::decode(&message) {
                    decoded_message = tmp;
                } else {
                    println!("this is not a erc20 transfer");
                }
                decoded_message.price = to_fix(decoded_message.price,4);
                decoded_message.amount = to_fix(decoded_message.amount,4);
               // println!("new order {:?},---{}--{}",decoded_message,message,to_fix(decoded_message.amount,4));
                // todo:checkout available amount
                let orders = engine::matched(decoded_message);
                println!("matched {:?}",orders);
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::ORDER_CONSUMER.lock().unwrap().consume_messageset(ms);
        }
        crate::ORDER_CONSUMER.lock().unwrap().commit_consumed();
    }
}

pub fn flush_start() {
    loop {
        unsafe {
            if  crate::trades.len() > 0{
                let pending_trades = &crate::trades;
                for trade in pending_trades {
                    println!("get an engine trade {:?}", trade);
                    // todo:update order
                    // todo:insert trade
                        //created_at: time::Duration::from_secs(now.now()),
                        // let dt: DateTime<Utc> = Utc::now();       // e.g. `2014-11-28T12:45:59.324310806Z`

                        println!("00-----------------===={}---",get_current_time());

                    let mut taker_order = crate::models::get_order(&trade.taker_order_id);
                    let  mut maker_order = crate::models::get_order(&trade.maker_order_id);
                    println!("3333----{:?}{:?}---33",taker_order,maker_order);
                    flush::update_order(& mut taker_order,&trade);
                    flush::update_order(& mut maker_order,&trade);

                    trades.pop();
                }
               // flush::insert_trade(&taker_order,&maker_order,pending_trades);
                continue;
            }
            println!("have no engine trade {:?}", crate::trades);
            std::thread::sleep(std::time::Duration::new(5,0));
        }
    }
}