use std::cmp::Ord;
use std::collections::BTreeMap;
use serde::Deserialize;
use std::env;
use std::ops::Mul;
use std::any::Any;
use crate::models::*;
use rustc_serialize::json;
use crate::util::to_fix;


#[derive(Deserialize, Debug)]
struct Transfer {
    private_key: String,
    fromaccount: String,
    toaccount: String,
    amount: f64,
    token: String,
}

fn add_available_orders(partner_available_orders: &mut Vec<EngineOrder>, new_order: EngineOrder) {
    let mut index = 0;
    unsafe {
        loop {
            println!("kkk3333---{}--{}", new_order.price, partner_available_orders[index].price);
            if new_order.price >= partner_available_orders[index].price {
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

pub fn matched(mut taker_order: EngineOrder) -> Vec<EngineOrder> {
    // todo：匹配订单
    println!("taker_order = {:?}", taker_order);
    let mut matched_orders: Vec<EngineOrder> = Vec::new();
    unsafe {
        let mut sum_matched: f64 = 0.0;
        let mut matched_amount: f64 = 0.0;
        let mut index = 0;
        if crate::available_buy_orders.len() == 0 || crate::available_sell_orders.len() == 0 {
            return matched_orders;
        }
        loop {
            let mut current_sell_amount = crate::available_buy_orders[0].amount.clone();
            let current_available_amount = to_fix(taker_order.amount - sum_matched, 4);
            let mut next_available_amount = to_fix(current_available_amount - current_sell_amount, 4);
            let mut price_gap = 0.0;
            let mut opponents_available_orders = &mut Default::default();
            let mut partner_available_orders = &mut Default::default();
            if taker_order.side == "sell" {
                opponents_available_orders = &mut crate::available_buy_orders;
                partner_available_orders = &mut crate::available_sell_orders;
                price_gap = taker_order.price - opponents_available_orders[0].price;
            } else {
                opponents_available_orders = &mut crate::available_sell_orders;
                partner_available_orders = &mut crate::available_buy_orders;
                price_gap = crate::available_sell_orders[0].price - taker_order.price;
            }
            if current_available_amount > 0.0 && price_gap <= 0.0 {
                // println!("kkk000----{}---{}----{}-", current_available_amount, taker_order.price, crate::available_buy_orders[0].price);
                if next_available_amount > 0.0 {
                    matched_amount = current_sell_amount;
                    matched_orders.push(opponents_available_orders[0].clone());
                    opponents_available_orders.remove(0);
                } else if next_available_amount < 0.0 {
                    matched_amount = current_available_amount;
                    //crate::available_sell_orders[0].amount -= current_available_amount;
                    opponents_available_orders[0].amount = to_fix(current_sell_amount - current_available_amount, 4);

                    let mut matched_order = opponents_available_orders[0].clone();
                    matched_order.amount = current_available_amount;
                    matched_orders.push(matched_order);
                    break;
                } else {
                    matched_orders.push(opponents_available_orders[0].clone());
                    opponents_available_orders.remove(0);
                    break;
                }
            } else if current_available_amount > 0.0 && price_gap > 0.0 {
                taker_order.amount = current_available_amount;
                println!("kkk2222---{:?}---{}-", taker_order, current_available_amount);
                add_available_orders(partner_available_orders, taker_order);
                break;
            } else {
                break;
            }
            sum_matched = to_fix(sum_matched + matched_amount, 4);
        }
    }
    matched_orders
}

pub fn make_trades() {
//todo: 组装撮合结果
}

pub fn write_PG() {
//todo：撮合结果落表，插入trades，更新orders
}

