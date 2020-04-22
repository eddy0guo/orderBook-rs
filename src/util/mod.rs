use rust_decimal::Decimal;
use num::ToPrimitive;
use chrono::prelude::*;
use chrono::offset::LocalResult;
use std::any::Any;
use std::fmt::Debug;
use crate::models::TradeInfo;
// use crate::consume::engine::EngineTrade;

pub fn to_fix(mut number:f64,mut precision:u32) -> f64{
    let times = 10_u32.pow(precision);
    let number_tmp = number * times as f64;
    let real_number = number_tmp.round();
    let decimal_number = Decimal::new(real_number as i64, precision);
    let scaled = decimal_number.to_f64().unwrap();
    scaled
}

pub fn get_current_time() -> String{
    let dt: DateTime<Local> = Local::now();
    dt.format("%Y-%m-%d %H:%M:%S.%f").to_string()
}
/*
pub struct TradeInfo {
    id: i32,
    transaction_id: i32,
    transaction_hash: String,
    status: String,
    market_id: String,
    maker: String,
    taker: String,
    price: String,
    amount: String,
    taker_side: String,
    maker_order_id: String,
    taker_order_id: String,
}
*/
pub fn struct2array<T:Any+Debug>(value: &T) -> Vec<String>{
    let mut trade_vec: Vec<String>= vec![];
    let value = value as &Any;
    match value.downcast_ref::<TradeInfo>() {
        Some(trade) => {
            trade_vec.push(trade.id.to_string());
            trade_vec.push(trade.transaction_id.to_string());
            trade_vec.push(trade.transaction_hash.to_string());
            trade_vec.push(trade.status.to_string());
            trade_vec.push(trade.market_id.to_string());
            trade_vec.push(trade.maker.to_string());
            trade_vec.push(trade.taker.to_string());
            trade_vec.push(trade.maker_order_id.to_string());
            trade_vec.push(trade.taker_order_id.to_string());
        }
        None => (),
    };
    trade_vec
}