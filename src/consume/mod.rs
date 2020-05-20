pub(crate) mod engine;
mod flush;

extern crate kafka;

use std::cmp::Ord;
use std::collections::BTreeMap;

use super::models::*;
use super::util::*;
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
            std::thread::sleep(std::time::Duration::new(0, 100 * 1000 * 1000));
            continue;
        }
        /**
        unsafe {
            println!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
                     crate::available_buy_orders.len(), crate::available_sell_orders.len(),
                     crate::available_buy_orders, crate::available_sell_orders);
        }**/
        println!("0001---");
        for ms in mss.iter() {
            println!("0002---");
            for m in ms.messages() {
                println!("0003---");
                let mut message = String::new();
                let message = String::from_utf8_lossy(m.value);
                let mut decoded_message: EngineOrder = Default::default();
                let mut decoded_order: OrderInfo = Default::default();
                decoded_order = json::decode(&message).unwrap_or_else(|err| {
                    println!("decode message error {:?}", err);
                    decoded_order
                });
                println!("0004---");
                decoded_order.price = decoded_order.price.to_fix(4);
                decoded_order.amount = decoded_order.amount.to_fix(4);
                //println!("new order--hello- {:?},---{}--{}",decoded_message,message,decoded_message.amount.to_fix(4));
                // todo:checkout available amount
                println!("0005---");
                engine::matched(decoded_order);
                println!("0006---");
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::ORDER_CONSUMER.lock().unwrap().consume_messageset(ms);
            println!("0007---");
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
    let mut index = 0;
    loop {
        unsafe {
            index += 1;
            if index % 100 == 0  {
                println!("time={:?}--trades={:?}--", crate::util::get_current_time(),crate::trades);
            }
            if crate::trades.len() > 0 {
                println!("\n\n\n\n\n\n");
                let mut trades_arr: Vec<Vec<String>> = Default::default();
                println!("start flush engine result {:?}", crate::trades);
                let pending_trades = crate::trades.clone();
                let mut index = 0;
                let mut current_transaction_id = crate::models::get_max_transaction_id();
                let mut matched_num = crate::models::count_matched_trades();
                let mut matched_trade_batch = 1;
                if (matched_num != 0) {
                    matched_trade_batch = matched_num / 10 + 1;
                }
                current_transaction_id += matched_trade_batch;

                for trade in pending_trades {
                    println!("trade-1----={:#?}", trade);
                    current_transaction_id += index / 10;
                    //let mut taker_order = crate::models::get_order(&trade.taker_order_id);
                    let mut taker_order = crate::models::get_order(&trade.taker_order_id);
                    let mut maker_order = crate::models::get_order(&trade.maker_order_id);
                    println!("ttttttttt {:?}---{:?}", taker_order, maker_order);


                    flush::update_maker(&mut maker_order, &trade);
                    let trade_arr = flush::generate_trade(
                        &taker_order,
                        &maker_order,
                        &trade,
                        current_transaction_id,
                    );
                    println!("generate_trade {:?}", trade_arr);
                    println!("flush--0002");
                    trades_arr.push(trade_arr);
                    println!("flush--0003---trade.taker_order_id={},gloable_tratde={:?}-", trade.taker_order_id, crate::trades);
                    // trades.remove(0);
                    crate::trades.retain(|x| !(x.taker_order_id == trade.taker_order_id &&
                        x.maker_order_id == trade.maker_order_id));
                    println!("flush--0004---trade.taker_order_id={},gloable_tratde={:?}-", trade.taker_order_id, crate::trades);
                    index += 1;
                }
                println!("insert trades_arr {:#?}", trades_arr);
                insert_trade2(&mut trades_arr);
                continue;
            }
            //println!("3333----{:?}{:?}---33", taker_order, maker_order);
            //println!("have no engine trade {:?}", crate::trades);
            // println!("have no engine trade {:?}", crate::util::get_current_time());
            std::thread::sleep(std::time::Duration::new(0, 100 * 1000 * 1000));
            // println!("have no engine trade2 {:?}", crate::util::get_current_time());
        }
    }
}
