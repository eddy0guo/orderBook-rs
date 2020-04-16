use std::cmp::Ord;
use std::collections::BTreeMap;

use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde::Deserialize;
use std::env;
use std::ops::Mul;
use std::any::Any;
use kafka::error::Error as KafkaError;


pub fn update_balance() -> bool {
    // todo:更新redis余额
    true
}
