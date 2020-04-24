use crate::models::TradeInfo;
use chrono::offset::LocalResult;
use chrono::prelude::*;
use num::ToPrimitive;
use rust_decimal::Decimal;
use std::any::Any;
use std::fmt::Debug;
// use crate::consume::engine::EngineTrade;

pub trait MathOperation {
    fn to_fix(&self,precision: u32) -> f64;
}

impl MathOperation for f64{
    fn to_fix(&self, precision: u32) -> f64{
        let times = 10_u32.pow(precision);
        let number_tmp = self * times as f64;
        let real_number = number_tmp.round();
        let decimal_number = Decimal::new(real_number as i64, precision);
        let scaled = decimal_number.to_f64().unwrap();
        scaled
    }
}

pub fn get_current_time() -> String {
    let dt: DateTime<Local> = Local::now();
    dt.format("%Y-%m-%d %H:%M:%S.%f").to_string()
}

pub fn struct2array<T: Any + Debug>(value: &T) -> Vec<String> {
    let mut trade_vec: Vec<String> = vec![];
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
            trade_vec.push(trade.price.to_string());
            trade_vec.push(trade.amount.to_string());
            trade_vec.push(trade.taker_side.to_string());
            trade_vec.push(trade.maker_order_id.to_string());
            trade_vec.push(trade.taker_order_id.to_string());
            trade_vec.push(trade.updated_at.to_string());
            trade_vec.push(trade.created_at.to_string());
        }
        None => (),
    };
    trade_vec
}
