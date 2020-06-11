use crate::consume::flush;
use crate::models::{postgresql};
use crate::util::*;
use async_std::task;
use futures::executor::block_on;
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

pub fn cancel_order(order_info: &postgresql::OrderInfo) {
    let mut book = flush::AddBook {
        asks: Vec::new(),
        bids: Vec::new(),
    };
    let mut order = postgresql::UpdateOrder {
        id: order_info.id.clone(),
        trader_address: order_info.trader_address.clone(),
        status: order_info.status.clone(),
        amount: order_info.amount,
        available_amount: order_info.available_amount,
        confirmed_amount: order_info.confirmed_amount,
        canceled_amount: order_info.canceled_amount,
        pending_amount: order_info.pending_amount,
        updated_at: get_current_time(),
    };
    let mut partner_available_orders = &mut Default::default();
    unsafe {
        if order_info.side == "buy" {
            partner_available_orders = &mut crate::available_sell_orders;
            book.bids.push([order_info.price, -order.amount]);
        } else {
            partner_available_orders = &mut crate::available_buy_orders;
            book.asks.push([order_info.price, -order.amount]);
        }
        //撮合数组减掉相应
        partner_available_orders.retain(|x| x.id != order.id);
        //增量推送页减去
        flush::push_add_book(book);
        //取消订单异步落表
        order.available_amount = 0.0;
        //fixme 这里临时为了适配老的逻辑，先这样后边改为canceled_amount
        order.canceled_amount = order.amount;
        order.updated_at = get_current_time();
        order.status = "cancled".to_string();
        info!("update canceled order {:#?}", order);
        task::spawn(postgresql::update_order2(order));
    }
}

fn update_available_orders(
    partner_available_orders: &mut Vec<postgresql::EngineOrder>,
    taker_order: postgresql::OrderInfo,
) {
    let new_order = postgresql::EngineOrder {
        id: taker_order.id.clone(),
        price: taker_order.price,
        amount: taker_order.amount,
        side: taker_order.side.clone(),
        created_at: taker_order.created_at.clone(),
    };
    let mut index = 0;
    let mut book = flush::AddBook {
        asks: Vec::new(),
        bids: Vec::new(),
    };
    let order_info = crate::util::struct2array(&taker_order);
    task::spawn(postgresql::insert_order2(order_info));

    unsafe {
        let mut price_gap = 0.0;
        if partner_available_orders.len() == 0 {
            partner_available_orders.push(new_order);
            //info!("add_available_orders 2222= {:?}", partner_available_orders);
            return;
        }
        if new_order.side == "buy" {
            book.bids.push([new_order.price, new_order.amount]);
            price_gap = (new_order.price - partner_available_orders[index].price).to_fix(4);
        } else {
            book.asks.push([new_order.price, new_order.amount]);
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
    flush::push_add_book(book);
}

pub fn matched(mut taker_order: postgresql::OrderInfo) {
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
                update_available_orders(partner_available_orders, taker_order.clone());
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

        if taker_order.available_amount > 0.0 {
            update_available_orders(partner_available_orders, taker_order.clone());
        }
    }
}

pub fn generate_trade(taker_order: &postgresql::OrderInfo, maker_order: &postgresql::EngineOrder, matched_amount: f64) {
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
