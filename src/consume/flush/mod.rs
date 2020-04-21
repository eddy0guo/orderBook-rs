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

pub fn insert_trade(taker_order: &EngineOrder, maker_order: &EngineOrder, engine_trade: &Vec<EngineTrade>) -> bool {
    // todo:更新redis余额
    true
}
