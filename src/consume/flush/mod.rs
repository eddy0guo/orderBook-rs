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
pub fn update_order(order:&mut UpdateOrder,engine_trade:&EngineTrade) -> bool {
    //created_at: time::Duration::from_secs(now.now()),
    let sys_time = SystemTime::now();
    // todo:更新redis余额
    if engine_trade.taker_side == "buy" {
        order.available_amount =  to_fix(order.available_amount - engine_trade.amount,4);
        order.pending_amount = to_fix(order.pending_amount + engine_trade.amount,4);
        if  order.available_amount > 0.0 && order.available_amount < order.amount{
            order.status = "partial_filled".to_string();
        }else if  order.available_amount == order.amount{
            order.status = "pending".to_string();
        }else {
            println!("Other circumstances that were not considered, or should not have occurred");
        }
        println!("---sys_time----{:?}--4",sys_time.to_owned());
        //order.updated_at =
        //super::models::update_order(order);
    }else {
        println!("----sys_time---{:?}--4",sys_time.to_owned());
    }

    true
}

pub fn insert_trade(taker_order:&EngineOrder,maker_order:&EngineOrder,engine_trade:&Vec<EngineTrade>) -> bool {
    // todo:更新redis余额
    true
}
