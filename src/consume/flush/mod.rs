use std::cmp::Ord;
use std::collections::BTreeMap;
use crate::models::*;
use crate::util::*;
use super::engine::EngineTrade;
use std::time::SystemTime;


use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde::Deserialize;
use std::env;
use std::ops::Mul;
use std::any::Any;
use kafka::error::Error as KafkaError;


//  "pending","partial_filled","cancled","full_filled" or ""
pub fn update_order(order: &mut UpdateOrder, engine_trade: &EngineTrade) -> bool {
    // todo:更新redis余额
    order.available_amount = to_fix(order.available_amount - engine_trade.amount, 4);
    order.pending_amount = to_fix(order.pending_amount + engine_trade.amount, 4);
    order.updated_at = get_current_time();
    if order.available_amount > 0.0 && order.available_amount < order.amount {
        order.status = "partial_filled".to_string();
    } else if order.available_amount == 0.0 {
        order.status = "full_filled".to_string();
    } else {
        println!("Other circumstances that were not considered, or should not have occurred");
    }
    crate::models::update_order(order);
    true
}

pub fn generate_trade(taker_order: &UpdateOrder, maker_order: &UpdateOrder, engine_trade: &EngineTrade) -> bool {
    // todo:更新redis余额
    unsafe {
        let trade = TradeInfo {
            id: 0,
            transaction_id: 1,
            transaction_hash: "33".to_string(),
            status: "matched".to_string(),
            market_id: crate::market_id.to_string(),
            maker: maker_order.trader_address.clone(),
            taker: taker_order.trader_address.clone(),
            price: engine_trade.price,
            amount: engine_trade.amount,
            taker_side: engine_trade.taker_side.to_string(),
            maker_order_id: engine_trade.maker_order_id.to_string(),
            taker_order_id: engine_trade.taker_order_id.to_string()
        };
        let result = struct2array(&trade);
        println!("insert_trade-struct2array={:?}--",result);
    }
    true
}
