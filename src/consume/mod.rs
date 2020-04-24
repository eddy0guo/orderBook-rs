pub(crate) mod engine;
mod flush;

extern crate kafka;

use std::cmp::Ord;
use std::collections::BTreeMap;

use super::models::*;
use super::util::*;
use crate::trades;
use chrono::offset::LocalResult;
use chrono::prelude::*;
use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;
use rustc_serialize::json;
use serde::Deserialize;
use std::any::Any;
use std::env;
use std::ops::Mul;

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
        /**
        unsafe {
            println!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
                     crate::available_buy_orders.len(), crate::available_sell_orders.len(),
                     crate::available_buy_orders, crate::available_sell_orders);
        }**/

        for ms in mss.iter() {
            for m in ms.messages() {
                let mut message = String::new();
                let message = String::from_utf8_lossy(m.value);
                let mut decoded_message: EngineOrder = Default::default();
                if let Ok(tmp) = json::decode(&message) {
                    decoded_message = tmp;
                } else {
                    println!("decode order message failed");
                }
                decoded_message.price = decoded_message.price.to_fix( 4);
                decoded_message.amount = decoded_message.amount.to_fix( 4);
                //println!("new order--hello- {:?},---{}--{}",decoded_message,message,decoded_message.amount.to_fix(4));
                // todo:checkout available amount
                engine::matched(decoded_message);
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::ORDER_CONSUMER.lock().unwrap().consume_messageset(ms);
        }
        /**
        unsafe {
            println!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
                     crate::available_buy_orders.len(), crate::available_sell_orders.len(),
                     crate::available_buy_orders, crate::available_sell_orders);
        }**/
        crate::ORDER_CONSUMER.lock().unwrap().commit_consumed();
    }
}

pub fn flush_start() {
    loop {
        unsafe {
            let mut trades_arr: Vec<Vec<String>> = Default::default();
            if crate::trades.len() > 0 {
                let pending_trades = &crate::trades;
                let index = 0;
                let current_transaction_id = crate::transaction_id;
                for trade in pending_trades {
                    let times = index / 100 + 1;
                    crate::transaction_id = current_transaction_id + times;
                    println!("current transaction_id is {}-----",crate::transaction_id);
                    println!("get an engine trade {:?}", trade);
                    // todo:update order
                    // todo:insert trade
                    let mut taker_order = crate::models::get_order(&trade.taker_order_id);
                    let mut maker_order = crate::models::get_order(&trade.maker_order_id);
                    flush::update_order(&mut taker_order, &trade);
                    flush::update_order(&mut maker_order, &trade);
                    let trade_arr = flush::generate_trade(&taker_order, &maker_order, &trade,crate::transaction_id);
                    trades_arr.push(trade_arr);
                    trades.pop();
                }
                insert_trade(&mut trades_arr);
                continue;
            }
            //println!("3333----{:?}{:?}---33", taker_order, maker_order);
            println!("have no engine trade {:?}", crate::trades);
            std::thread::sleep(std::time::Duration::new(0, 100));
        }
    }
}
