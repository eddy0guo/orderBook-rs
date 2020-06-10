use super::engine::EngineTrade;
use crate::models::*;
use crate::util::*;
use std::cmp::Ord;
use std::collections::BTreeMap;
use std::time::SystemTime;
use std::fmt::Write;
use kafka::producer::{Producer, Record, RequiredAcks};

extern crate kafka;

use std::time::Duration;
use kafka::error::Error as KafkaError;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};


use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde::Deserialize;
use std::any::Any;
use std::env;
use std::ops::Mul;
use rustc_serialize::json;

#[derive(RustcEncodable, Clone)]
pub struct AddBook {
    pub asks: Vec<[f64;2]>,
    pub bids: Vec<[f64;2]>,
}
#[derive(RustcEncodable, Clone)]
struct MarketUpdateBook{
    id:String,
    data:AddBook,
}

#[derive(RustcEncodable, Clone)]
struct LastTrade {
    price: f64,
    amount: f64,
    taker_side: String,
    updated_at: String,
}
#[derive(RustcEncodable, Clone)]
struct marketLastTrades{
    data: Vec<LastTrade>,
    id: String,
}

//  "pending","partial_filled","cancled","full_filled" or ""
pub fn update_maker(order: &mut UpdateOrder, engine_trade: &EngineTrade) -> bool {
    // todo:更新redis余额
    order.available_amount = (order.available_amount - engine_trade.amount).to_fix(4);
    order.pending_amount = (order.pending_amount + engine_trade.amount).to_fix(4);
    order.updated_at = get_current_time();
    if order.available_amount > 0.0 && order.available_amount < order.amount {
        order.status = "partial_filled".to_string();
    } else if order.available_amount == 0.0 {
        order.status = "full_filled".to_string();
    } else {
        info!("Other circumstances that were not considered, or should not have occurred");
    }
    crate::models::update_order(order);
    true
}

pub fn insert_taker(taker_order: &mut OrderInfo, engine_trade: &EngineTrade) -> bool {
    // todo:更新redis余额
    taker_order.available_amount = (taker_order.available_amount - engine_trade.amount).to_fix(4);
    taker_order.pending_amount = (taker_order.pending_amount + engine_trade.amount).to_fix(4);
    taker_order.updated_at = get_current_time();
    if taker_order.available_amount > 0.0 && taker_order.available_amount < taker_order.amount {
        taker_order.status = "partial_filled".to_string();
    } else if taker_order.available_amount == 0.0 {
        taker_order.status = "full_filled".to_string();
    } else {
        error!("Other circumstances that were not considered, or should not have occurred");
    }
    let order_info = struct2array(taker_order);
    crate::models::insert_order2(order_info);
    true
}

pub fn generate_trade(
    taker: &str,
    maker_order: &UpdateOrder,
    engine_trade: &EngineTrade,
    transaction_id: i32,
) -> Vec<String> {
    // todo:更新redis余额
    //fixme::默认值设计
    unsafe {
        let mut trade = TradeInfo {
            id: "".to_string(),
            transaction_id,
            transaction_hash: "".to_string(),
            status: "matched".to_string(),
            market_id: crate::market_id.clone(),
            maker: maker_order.trader_address.clone(),
            taker: taker.to_string(),
            price: engine_trade.price,
            amount: engine_trade.amount,
            taker_side: engine_trade.taker_side.clone(),
            maker_order_id: engine_trade.maker_order_id.clone(),
            taker_order_id: engine_trade.taker_order_id.clone(),
            updated_at: get_current_time(),
            created_at: get_current_time(),
        };
        let data = format!(
            "{}{}{}{}{}{}{}{}{}",
            trade.market_id,
            trade.maker,
            trade.taker,
            trade.price,
            trade.amount,
            trade.taker_side,
            trade.maker_order_id,
            trade.taker_order_id,
            trade.created_at
        );
        let txid = sha256(data);
        trade.id = txid;
        let trade_arr = struct2array(&trade);
        trade_arr
    }
}

/***
try!(producer.send(&Record{
        topic: topic,
        partition: -1,
        key: (),
        value: data,
    }));

**/

pub fn push_add_book(add_book: AddBook){
    unsafe {
        let add_message = MarketUpdateBook{
            id: crate::market_id.clone(),
            data:add_book,
        };
        //demo url https://docs.rs/rdkafka/0.23.1/rdkafka/producer/base_producer/struct.BaseProducer.html
        let add_book_encode = json::encode(&add_message).unwrap();
        // addTradesQueue
        crate::ADD_PRODUCER.lock().unwrap().send(
            BaseRecord::to("addOrderBookQueue")
                .payload(&add_book_encode)
                .key("mistBook"),
        ).expect("Failed to enqueue");
        crate::ADD_PRODUCER.lock().unwrap().flush(Duration::from_secs(1));
    }

}

pub fn push_add_trades(trades : &Vec<EngineTrade>){
    let trades_tmp = trades.clone();
    let mut lastTrades:Vec<LastTrade> = Vec::new();
    for trade in trades_tmp {
        let last_tmp = LastTrade{
            price:trade.price,
            amount:trade.amount,
            taker_side:trade.taker_side,
            updated_at: get_current_time(),
        };
        lastTrades.push(last_tmp);
    }
    unsafe {
        let add_message = marketLastTrades{
            id: crate::market_id.clone(),
            data:lastTrades,
        };
        //demo url https://docs.rs/rdkafka/0.23.1/rdkafka/producer/base_producer/struct.BaseProducer.html
        let add_book_encode = json::encode(&add_message).unwrap();
        crate::ADD_PRODUCER.lock().unwrap().send(
            BaseRecord::to("addTradesQueue")
                .payload(&add_book_encode)
                .key("mistTrade"),
        ).expect("Failed to enqueue");
        crate::ADD_PRODUCER.lock().unwrap().flush(Duration::from_secs(1));
    }

}

pub fn compute_order_book_updates(trades: &Vec<EngineTrade>) -> AddBook{
    let mut  book = AddBook{
        asks: Vec::new(),
        bids: Vec::new(),
    };
    for  trade in trades  {
        if trade.taker_side == "sell".to_string(){
            let mut priceExsit = false;
            for bid in book.bids.iter_mut() {
                if (trade.price == bid[0]) {
                    bid[1] = bid[1] - trade.amount;
                    priceExsit = true;
                    break;
                }
            }
            if !priceExsit {
                book.bids.push([trade.price, -trade.amount]);
            }

        }else if trade.taker_side == "buy".to_string(){
            let mut priceExsit = false;
            for ask in book.asks.iter_mut() {
                if (trade.price == ask[0]) {
                    ask[1] = ask[1] - trade.amount;
                    priceExsit = true;
                    break;
                }
            }
            if !priceExsit {
                book.asks.push([trade.price, -trade.amount]);
            }

        }else {
            error!("unknown side {}",trade.taker_side)
        }
    }
    book
}