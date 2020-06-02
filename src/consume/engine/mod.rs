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
use futures::executor::block_on;
// use async_std::task;



#[derive(Deserialize, Debug, Clone)]
pub struct EngineTrade {
    pub taker_order_id: String,
    pub maker_order_id: String,
    pub taker_side: String,
    pub amount: f64,
    pub price: f64,
    pub market_id: String,
    pub taker: String,
}

fn add_available_orders(partner_available_orders: &mut Vec<EngineOrder>, new_order: EngineOrder) {
    let mut index = 0;
    unsafe {
        let mut price_gap = 0.0;
        if partner_available_orders.len() == 0 {
            partner_available_orders.push(new_order);
            //info!("add_available_orders 2222= {:?}", partner_available_orders);

            return;
        }
        // info!("add_available_orders333 = {:?}", partner_available_orders);
        if new_order.side == "buy" {
            price_gap = (new_order.price - partner_available_orders[index].price).to_fix(4);
        } else {
            price_gap = (partner_available_orders[index].price - new_order.price).to_fix(4);
        }
        loop {
            if price_gap >= 0.0 {
                //info!("222222222222----- price_gap={}---index={}--partner_available_orders={:?}-\n", price_gap, index, partner_available_orders);
                partner_available_orders.insert(index, new_order);
                break;
            }
            if index == partner_available_orders.len() - 1 {
                //info!("333333----- price_gap={}---index={}--partner_available_orders={:?}-\n", price_gap, index, partner_available_orders);
                partner_available_orders.insert(index + 1, new_order);
                break;
            }
            index += 1;
        }
    }
}

pub fn matched(mut taker_order: OrderInfo) {
    unsafe {
        // info!("start match_order = {:?}---opponents_available_orders={:?}", taker_order, crate::available_buy_orders);
    }
    unsafe {
        let mut sum_matched: f64 = 0.0;
        let mut matched_amount: f64 = 0.0;
        let mut index = 0;
        let mut opponents_available_orders = &mut Default::default();
        let mut partner_available_orders = &mut Default::default();
        let mut price_gap = 0.0;

        loop {
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
                let mut order_info = crate::util::struct2array(&taker_order);
                insert_order2(&mut order_info);
                let taker_order2 = EngineOrder {
                    id: taker_order.id,
                    price: taker_order.price,
                    amount: taker_order.amount,
                    side: taker_order.side,
                    created_at: taker_order.created_at,
                };
                add_available_orders(partner_available_orders, taker_order2);
                return;
            }

            let mut current_opponents_amount = opponents_available_orders[0].amount.clone();
            let current_available_amount = (taker_order.amount - sum_matched).to_fix(4);
            if current_available_amount > 0.0 && price_gap <= 0.0 {
                if current_available_amount < current_opponents_amount {
                    matched_amount = current_available_amount;
                } else {
                    matched_amount = current_opponents_amount;
                }

                taker_order.available_amount =
                    (taker_order.available_amount - matched_amount).to_fix(4);
                taker_order.pending_amount =
                    (taker_order.pending_amount + matched_amount).to_fix(4);
                opponents_available_orders[0].amount =
                    (opponents_available_orders[0].amount - matched_amount).to_fix(4);

                generate_trade(&taker_order, &opponents_available_orders[0], matched_amount);
                if opponents_available_orders[0].amount == 0.0 {
                    opponents_available_orders.remove(0);
                }
            } else {
                break;
            }
            sum_matched = (sum_matched + matched_amount).to_fix(4);
            info!("match result {:?}", crate::trades);
        }

        if taker_order.available_amount > 0.0 && taker_order.pending_amount == 0.0 {
            taker_order.status = "pending".to_string();
        } else if taker_order.available_amount > 0.0 && taker_order.pending_amount > 0.0 {
            taker_order.status = "partial_filled".to_string();
        } else if taker_order.available_amount == 0.0 && taker_order.pending_amount > 0.0 {
            taker_order.status = "full_filled".to_string();
        } else {
            info!("unknown case")
        }

        let mut order_info = crate::util::struct2array(&taker_order);
        let insert_order_future = insert_order2(&mut order_info);
        //task::spawn(insert_order2(&mut order_info));

        if taker_order.available_amount > 0.0 {
            let taker_order2 = EngineOrder {
                id: taker_order.id.clone(),
                price: taker_order.price,
                amount: taker_order.available_amount,
                side: taker_order.side.clone(),
                created_at: taker_order.created_at.clone(),
            };
            add_available_orders(partner_available_orders, taker_order2);
        }
    }
}

pub fn generate_trade(taker_order: &OrderInfo, maker_order: &EngineOrder, matched_amount: f64) {
    let taker_order2 = taker_order.clone();
    let maker_order2 = maker_order.clone();

    unsafe {
        let mut trade = EngineTrade {
            market_id: crate::market_id.clone(),
            price: maker_order2.price,
            amount: matched_amount,
            taker_side: taker_order2.side.clone(),
            maker_order_id: maker_order2.id,
            taker_order_id: taker_order2.id,
            taker: taker_order2.trader_address,
        };
        // 这里加上takerorder字段
        crate::trades.push(trade);
        // info!("finished push trades {:?}", crate::trades);
    }
}
