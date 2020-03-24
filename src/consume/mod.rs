mod bridge;

extern crate kafka;

use std::cmp::Ord;
use std::collections::BTreeMap;

use jsonrpc_http_server::jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde::Deserialize;

use super::models::*;
use bridge::*;
use std::env;
use std::ops::Mul;
use std::any::Any;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;


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

pub fn order_start() {
    loop {
        let mss = crate::ORDER_STREAM.lock().unwrap().poll().unwrap();
        if mss.is_empty() {
            println!("No order messages available right now.");
            continue;
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                let mut message = String::new();
                // for a in m.value.iter() {
                //println!(" N: {:x?}", a);
                //signature_string.push(a);
                let message = String::from_utf8_lossy(m.value);
                // write!(message,"{}",message);
                println!("catch bridge messages {}", message);
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::ORDER_STREAM.lock().unwrap().consume_messageset(ms);
        }
        crate::ORDER_STREAM.lock().unwrap().commit_consumed();
    }
}

pub fn bridge_start() {
    loop {
        let mss = crate::BRIDGE_STREAM.lock().unwrap().poll().unwrap();
        if mss.is_empty() {
            println!("No bridge messages available right now.");
            continue;
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                let mut message = String::new();
                // for a in m.value.iter() {
                //println!(" N: {:x?}", a);
                //signature_string.push(a);
                let message = String::from_utf8_lossy(m.value);
                // write!(message,"{}",message);
                println!("catch bridge messages {}", message);
                update_balance();
                // println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, message);
            }
            let _ = crate::BRIDGE_STREAM.lock().unwrap().consume_messageset(ms);
        }
        crate::BRIDGE_STREAM.lock().unwrap().commit_consumed();
    }
}