mod flush;
mod engine;
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
        let mut mss = crate::ORDER_STREAM.lock().unwrap().poll().unwrap();
        if mss.is_empty() {
            // println!("No order messages available right now.");
            continue;
        }
        unsafe {
            println!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
                     crate::available_buy_orders.len(),crate::available_sell_orders.len(),
                     crate::available_buy_orders,crate::available_sell_orders);
        }
        for ms in mss.iter() {
            for m in ms.messages() {
                println!("matched ----555");
                let mut message = String::new();
                // for a in m.value.iter() {
                //println!(" N: {:x?}", a);
                //signature_string.push(a);
                let message = String::from_utf8_lossy(m.value);
               // println!("older order {}",message);
                let mut decoded_message: EngineOrder = Default::default();
                if let Ok(tmp) = json::decode(&message) {
                    decoded_message = tmp;
                } else {
                    println!("this is not a erc20 transfer");
                }
                decoded_message.price = to_fix(decoded_message.price,4);
                decoded_message.amount = to_fix(decoded_message.amount,4);
               // println!("new order {:?},---{}--{}",decoded_message,message,to_fix(decoded_message.amount,4));
                let orders = engine::matched(decoded_message);
                // write!(message,"{}",message);1
                println!("matched {:?}",orders);
                println!("matched ----444");
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            println!("matched1 ---111");
            let _ = crate::ORDER_STREAM.lock().unwrap().consume_messageset(ms);
            println!("matched2 ---222");

        }
        unsafe {
            println!("afterrrrr  available buy order len {},available sell order len {}---BUY{:?},---------SELL{:?}",
                     crate::available_buy_orders.len(),crate::available_sell_orders.len(),crate::available_buy_orders,
                     crate::available_sell_orders);
        }
        crate::ORDER_STREAM.lock().unwrap().commit_consumed();
        println!("matched3 ---333");
    }
}

pub fn flush_start() {
    loop {
        let mss = crate::BRIDGE_STREAM.lock().unwrap().poll().unwrap();
        if mss.is_empty() {
            println!("No flush messages available right now.");
            continue;
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                let mut message = String::new();
                // for a in m.value.iter() {
                //println!(" N: {:x?}", a);
                //signature_string.push(a);
                let message = String::from_utf8_lossy(m.value);
                // write!(message,"{}",message);
                //println!("catch flush messages33 {}", message);
               // update_balance();
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::BRIDGE_STREAM.lock().unwrap().consume_messageset(ms);
        }
        crate::BRIDGE_STREAM.lock().unwrap().commit_consumed();
    }
}