use crate::models::*;
use crate::util::MathOperation;
use kafka::producer::{Producer, Record, RequiredAcks};
use log::__private_api_enabled;
use rustc_serialize::json;
use serde::Deserialize;
use std::any::Any;
use std::cmp::Ord;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write;
use std::ops::Mul;
use std::ptr::null;
use std::rc::Rc;
use std::time::Duration;

#[derive(Deserialize, Debug)]
pub struct EngineTrade {
    pub taker_order_id: String,
    pub maker_order_id: String,
    pub taker_side: String,
    pub amount: f64,
    pub price: f64,
    pub market_id: String,
}

fn add_available_orders(partner_available_orders: &mut Vec<EngineOrder>, new_order: EngineOrder) {
    let mut index = 0;
    unsafe {
        let mut price_gap = 0.0;
        if partner_available_orders.len() == 0 {
            partner_available_orders.push(new_order);
            //println!("add_available_orders 2222= {:?}", partner_available_orders);

            return;
        }
        // println!("add_available_orders333 = {:?}", partner_available_orders);
        if new_order.side == "buy" {
            price_gap = new_order.price - partner_available_orders[index].price;
        } else {
            price_gap = partner_available_orders[index].price - new_order.price;
        }
        loop {
            if price_gap >= 0.0 {
                partner_available_orders.insert(index, new_order);
                break;
            }
            if index == partner_available_orders.len() - 1 {
                partner_available_orders.insert(index + 1, new_order);
                break;
            }
            index += 1;
        }
    }
}

pub fn matched(mut taker_order: EngineOrder) {
    println!("start match_order = {:?}", taker_order);
    unsafe {
        let mut sum_matched: f64 = 0.0;
        let mut matched_amount: f64 = 0.0;
        let mut index = 0;
        loop {
            let mut opponents_available_orders = &mut Default::default();
            let mut partner_available_orders = &mut Default::default();
            let mut price_gap = 0.0;
            if taker_order.side == "sell" {
                opponents_available_orders = &mut crate::available_buy_orders;
                partner_available_orders = &mut crate::available_sell_orders;
                if opponents_available_orders.len() != 0 {
                    price_gap = taker_order.price - opponents_available_orders[0].price;
                }
            } else {
                opponents_available_orders = &mut crate::available_sell_orders;
                partner_available_orders = &mut crate::available_buy_orders;
                if opponents_available_orders.len() != 0 {
                    price_gap = opponents_available_orders[0].price - taker_order.price;
                }
            }

            if opponents_available_orders.len() == 0 {
                add_available_orders(partner_available_orders, taker_order.clone());
                return;
            }

            let mut current_opponents_amount = opponents_available_orders[0].amount.clone();
            let current_available_amount = (taker_order.amount - sum_matched).to_fix(4);
            let mut next_available_amount =
                (current_available_amount - current_opponents_amount).to_fix(4);
            if current_available_amount > 0.0 && price_gap <= 0.0 {
                // println!("kkk000----{}---{}----{}-", current_available_amount, taker_order.price, crate::available_buy_orders[0].price);
                if next_available_amount > 0.0 {
                    matched_amount = current_opponents_amount;
                    generate_trade(&taker_order, &opponents_available_orders[0]);
                    opponents_available_orders.remove(0);
                } else if next_available_amount < 0.0 {
                    matched_amount = current_available_amount;
                    //crate::available_sell_orders[0].amount -= current_available_amount;
                    opponents_available_orders[0].amount = current_available_amount.to_fix(4);
                    let mut matched_order = opponents_available_orders[0].clone();
                    matched_order.amount = current_available_amount;
                    generate_trade(&taker_order, &opponents_available_orders[0]);
                    break;
                } else {
                    generate_trade(&taker_order, &opponents_available_orders[0]);
                    opponents_available_orders.remove(0);
                    break;
                }
            } else if current_available_amount > 0.0 && price_gap > 0.0 {
                taker_order.amount = current_available_amount;
                // println!("kkk2222---{:?}---{}-", taker_order, current_available_amount);
                add_available_orders(partner_available_orders, taker_order);
                break;
            } else {
                break;
            }
            println!("match result {:?}", crate::trades);
            sum_matched = (sum_matched + matched_amount).to_fix(4);
        }
    }
}

pub fn generate_trade(taker_order: &EngineOrder, maker_order: &EngineOrder) {
    let taker_order2 = taker_order.clone();
    let maker_order2 = maker_order.clone();

    unsafe {
        let mut trade = EngineTrade {
            market_id: crate::market_id.clone(),
            price: maker_order2.price,
            amount: maker_order2.amount,
            taker_side: taker_order2.side,
            maker_order_id: maker_order2.id,
            taker_order_id: taker_order2.id,
        };
        crate::trades.push(trade);
    }
}
