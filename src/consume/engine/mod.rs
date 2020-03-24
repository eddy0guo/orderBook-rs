use std::cmp::Ord;
use std::collections::BTreeMap;
use serde::Deserialize;
use std::env;
use std::ops::Mul;
use std::any::Any;

#[derive(Deserialize, Debug)]
struct Transfer {
    private_key: String,
    fromaccount: String,
    toaccount: String,
    amount: f64,
    token: String,
}

pub fn matched() {
    // todo：匹配订单
}

pub fn make_trades() {
    //todo: 组装撮合结果
}

pub fn write_PG() {
    //todo：撮合结果落表，插入trades，更新orders
}

