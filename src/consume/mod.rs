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
            // info!("No order messages available right now.");
            std::thread::sleep(std::time::Duration::new(0, 100 * 1000 * 1000));
            continue;
        }
        /**
        unsafe {
            info!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
                     crate::available_buy_orders.len(), crate::available_sell_orders.len(),
                     crate::available_buy_orders, crate::available_sell_orders);
        }**/
        for ms in mss.iter() {
            for m in ms.messages() {
                let mut message = String::new();
                let message = String::from_utf8_lossy(m.value);
                let mut decoded_message: EngineOrder = Default::default();
                let mut decoded_order: OrderInfo = Default::default();
                decoded_order = json::decode(&message).unwrap_or_else(|err| {
                    error!("decode message error {:?}", err);
                    decoded_order
                });
                info!("start engine at {}", crate::get_current_time());
                decoded_order.price = decoded_order.price.to_fix(4);
                decoded_order.amount = decoded_order.amount.to_fix(4);
                decoded_order.available_amount = decoded_order.available_amount.to_fix(4);
                decoded_order.confirmed_amount = decoded_order.confirmed_amount.to_fix(4);
                decoded_order.pending_amount = decoded_order.pending_amount.to_fix(4);
                decoded_order.canceled_amount = decoded_order.canceled_amount.to_fix(4);

                if decoded_order.status == "cancled".to_string() {
                    engine::cancel_order(&decoded_order);
                    continue;
                }

                // todo:checkout available amount
                engine::matched(decoded_order);
                info!("finished engine at {}", crate::get_current_time());
            }
            let _ = crate::ORDER_CONSUMER.lock().unwrap().consume_messageset(ms);
        }
        /**
        unsafe {
            info!("available buy order len {},available sell order len {},--BUY-{:?},--------SELL{:?}",
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
            if index % 1000 == 0 {
                info!(
                    "[FLUSH]:time={:?}--trades={:?}--",
                    crate::util::get_current_time(),
                    crate::trades
                );
            }
            if crate::trades.len() > 0 {
                info!("\n\n\n\n\n\n");
                let mut trades_arr: Vec<Vec<String>> = Default::default();
                info!("[FLUSH]:start flush engine result {:?}", crate::trades);
                let pending_trades = crate::trades.clone();
                let add_book = flush::compute_order_book_updates(&pending_trades);
                flush::push_add_book(add_book);
                flush::push_add_trades(&pending_trades);
                let mut index2 = 0;
                let mut index_add = 0;
                for trade in pending_trades {
                    index_add = index2 / 10;
                    //let mut taker_order = crate::models::get_order(&trade.taker_order_id);
                    info!("[FLUSH]: maker_order1={:?}", trade.maker_order_id);
                    let mut maker_order = crate::models::get_order(&trade.maker_order_id);
                    info!("[FLUSH]: maker_order2={:?}", trade.maker_order_id);
                    //info!("[FLUSH]: takerorder={:?},maker_order={:?}", taker_order, maker_order);
                    //可以异步
                    flush::update_maker(&mut maker_order, &trade);
                    let trade_arr =
                        flush::generate_trade(&trade.taker, &maker_order, &trade, index_add);
                    trades_arr.push(trade_arr);
                    // 在全局trades里移除该trade
                    crate::trades.retain(|x| {
                        !(x.taker_order_id == trade.taker_order_id
                            && x.maker_order_id == trade.maker_order_id)
                    });
                    index2 += 1;
                }

                //todo：关于laucher的队列的定序可以放到最后再确定
                info!("[FLUSH]:get_max_transaction_id--111");
                let mut current_transaction_id = crate::models::get_max_transaction_id();
                info!("[FLUSH]:get_max_transaction_id-22- ");
                let mut matched_num = crate::models::count_matched_trades();
                info!("[FLUSH]:count_matched_trades");
                let mut matched_trade_batch = 1;
                if (matched_num != 0) {
                    matched_trade_batch = matched_num / 10;
                }
                current_transaction_id += matched_trade_batch;
                info!("rades_arr.iter_mut start");
                for trade_arr in trades_arr.iter_mut() {
                    info!("trade_arr2222------{:?}-", trade_arr);
                    let transaction_id =
                        (*trade_arr[1]).parse::<i32>().unwrap() + current_transaction_id;
                    info!("trade_arr3333------{:?}-\n\n\n", transaction_id);
                    trade_arr[1] = transaction_id.to_string();
                }
                insert_trade2(&mut trades_arr);
                info!("[FLUSH]:insert trades22_arr {:?}", trades_arr);
                continue;
            }
            std::thread::sleep(std::time::Duration::new(0, 10 * 1000 * 1000));
        }
    }
}
