use postgres::{config::Config, error::Error, row::SimpleQueryRow, Client, NoTls};

extern crate rustc_serialize;

use rustc_serialize::json;

use serde::Deserialize;
use std::ops::Mul;

#[derive(Deserialize, Debug, Default)]
pub struct UpdateOrder {
    pub id: String,
    pub trader_address: String,
    pub status: String,
    pub amount: f64,
    pub available_amount: f64,
    pub confirmed_amount: f64,
    pub canceled_amount: f64,
    pub pending_amount: f64,
    pub updated_at: String,
}

#[derive(Deserialize, RustcDecodable, RustcEncodable, Debug, Default, Clone)]
pub struct EngineOrder {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: String,
    pub created_at: String,
}

#[derive(Deserialize, Debug, Default)]
pub struct TradeInfo {
    pub id: String,
    pub transaction_id: i32,
    pub transaction_hash: String,
    pub status: String,
    pub market_id: String,
    pub maker: String,
    pub taker: String,
    pub price: f64,
    pub amount: f64,
    pub taker_side: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub updated_at: String,
    pub created_at: String,
}

#[derive(Deserialize, RustcDecodable, Debug, Default, Clone)]
pub struct OrderInfo {
    pub id: String,
    pub trader_address: String,
    pub market_id: String,
    pub side: String,
    pub price: f64,
    pub amount: f64,
    pub status: String,
    //r#
    pub r#type: String,
    pub available_amount: f64,
    pub confirmed_amount: f64,
    pub canceled_amount: f64,
    pub pending_amount: f64,
    pub updated_at: String,
    pub created_at: String,
    pub signature: String,
    pub expire_at: u64,
}

#[derive(Deserialize, Debug, Default)]
pub struct MarketVolume {
    pub marketID: String,
    pub volume: f64,
}

use std::ptr::null;
use std::sync::Mutex;

pub fn get_max_transaction_id() -> i32 {
    let sql = format!(
        //"select transaction_id  from {} where status!='matched' order by transaction_id desc limit 1",
          "select transaction_id  from {} where status='pending' or status='successful'  order by transaction_id desc limit 1",
        crate::READ_TRADE_TABLE
    );
    let mut transaction_id: i32 = 0;
    let mut result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);

    if let Err(err) = result {
        info!("get max transaction_id failed {:?}", err);
        if !crate::restartDB() {
            return transaction_id;
        }
        result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        transaction_id = row.get(0);
    }
    transaction_id
}

pub fn count_matched_trades() -> i32 {
    let sql = format!(
        "SELECT  cast(count(1) as int4) FROM {} where status=\'matched\'",
        crate::READ_TRADE_TABLE
    );
    let mut num: i32 = 0;
    let mut result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);

    if let Err(err) = result {
        error!("count matched trades failed {:?},sql={}", err, sql);
        if !crate::restartDB() {
            return num;
        }
        result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        num = row.get(0);
    }
    num
}

pub fn insert_trade2(trades: &mut Vec<Vec<String>>) {
    insert_trade(trades, crate::WRITE_TRADE_TABLE);
    insert_trade(trades, crate::WRITE_TRADE_TMP_TABLE);
}

pub fn insert_trade(trades: &mut Vec<Vec<String>>, trade_table: &str) {
    let mut query = format!("insert into {} values(", trade_table);
    let mut tradesArr: Vec<&str> = Default::default();
    let mut index = 0;
    let trades_len = trades.len();
    // fixme:注入的写法暂时有问题，先直接拼接
    for trade in trades {
        let mut temp_value = "".to_string();
        for i in 0..trade.len() {
            if i < trade.len() - 1 {
                temp_value = format!("{}{},", temp_value, trade[i]);
            } else {
                temp_value = format!("{}{}", temp_value, trade[i]);
            }
        }
        if (index < trades_len - 1) {
            query = format!("{}{}),(", query, temp_value);
        } else {
            query = format!("{}{})", query, temp_value);
        }
        let mut str_trade: Vec<&str> = Default::default();
        for item in trade {
            str_trade.push(&*item);
        }
        tradesArr.append(str_trade.as_mut());
        index += 1;
    }
    let mut result = crate::CLIENTDB.lock().unwrap().execute(&*query, &[]);
    // let mut result = crate::CLIENTDB.lock().unwrap().execute(&*query, &tradesArr[0..tradesArr.len()]);
    if let Err(err) = result {
        error!("insert trade sql={} failed {:?}", query, err);
        if !crate::restartDB() {
            return;
        }
        //&[&bar, &baz],
        result = crate::CLIENTDB.lock().unwrap().execute(&*query, &[]);
    }
    let rows = result.unwrap();
    debug!(
        "insert trade successful insert {:?} rows,sql={}",
        rows, query
    );
}

pub  async fn insert_order2(trades: Vec<String>) {
    let mut trades_vec = trades;
    info!("start---------------------");
    insert_order(&mut trades_vec, crate::WRITE_ORDER_TABLE);
    info!("start2---------------------");
    insert_order(&mut trades_vec, crate::WRITE_ORDER_TMP_TABLE);
    info!("end---------------------");
}

pub fn insert_order(order_info: &mut Vec<String>, trade_table: &str) {
    let mut query = format!("insert into {} values(", trade_table);
    // fixme:注入的写法暂时有问题，先直接拼接
    for i in 0..order_info.len() {
        if i < order_info.len() - 1 {
            query = format!("{}{},", query, order_info[i]);
        } else {
            query = format!("{}{})", query, order_info[i]);
            //temp_value =+ '$' + (i + 14 * index);
        }
    }
    // info!("insert order successful insert,sql={}", query);
    let mut result = crate::CLIENTDB.lock().unwrap().execute(&*query, &[]);
    // let mut result = crate::CLIENTDB.lock().unwrap().execute(&*query, &tradesArr[0..tradesArr.len()]);
    if let Err(err) = result {
        info!("insert order sql={} failed {:?}", query, err);
        if !crate::restartDB() {
            return;
        }
        result = crate::CLIENTDB.lock().unwrap().execute(&*query, &[]);
    }
    let rows = result.unwrap();
}

pub fn update_order(order: &UpdateOrder) {
    // fixme:注入的写法暂时有问题，先直接拼接
    let sql = format!(
        "UPDATE {} SET (available_amount,confirmed_amount,\
         canceled_amount,pending_amount,status,updated_at)=\
         ({},confirmed_amount,canceled_amount,{},'{}','{}') WHERE id='{}'",
        crate::WRITE_ORDER_TABLE,
        order.available_amount,
        order.pending_amount,
        order.status,
        order.updated_at,
        order.id
    );
    let mut result = crate::CLIENTDB.lock().unwrap().execute(&*sql, &[]);
    if let Err(err) = result {
        info!("update order failed {:?},sql={}", err, sql);
        if !crate::restartDB() {
            return;
        }
        result = crate::CLIENTDB.lock().unwrap().execute(&*sql, &[]);
    }
    // info!("success update {} rows", result.unwrap());
    return;
}

pub fn get_order(id: &str) -> UpdateOrder {
    let sql = format!(
        "select id,trader_address,status,\
         cast(amount as float8),\
         cast(available_amount as float8),\
         cast(confirmed_amount as float8),\
         cast(canceled_amount as float8),\
         cast(pending_amount as float8),\
         cast(updated_at as text) \
         from {} where id=$1",
        crate::READ_ORDER_TABLE
    );
    let mut order: UpdateOrder = Default::default();
    let mut result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[&id]);
    if let Err(err) = result {
        info!("get order failed {:?},sql={}", err, sql);
        if !crate::restartDB() {
            return order;
        }
        result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[&id]);
    }
    //id 唯一，直接去第一个成员
    let rows = result.unwrap();
    order = UpdateOrder {
        id: rows[0].get(0),
        trader_address: rows[0].get(1),
        status: rows[0].get(2),
        amount: rows[0].get(3),
        available_amount: rows[0].get(4),
        confirmed_amount: rows[0].get(5),
        canceled_amount: rows[0].get(6),
        pending_amount: rows[0].get(7),
        updated_at: rows[0].get(8),
    };
    order
}

pub fn list_available_orders(side: &str, channel: &str) -> Vec<EngineOrder> {
    let mut sort_by = "ASC";
    if side == "buy" {
        sort_by = "DESC";
    }
    let sql = format!("select id,cast(price as float8),cast(available_amount as float8),side,cast(created_at as text) from {} \
    where market_id='{}' and available_amount>0 and side='{}' order by price {} ,created_at ASC", crate::READ_ORDER_TABLE, channel, side, sort_by);
    let mut orders: Vec<EngineOrder> = Vec::new();
    let mut result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    if let Err(err) = result {
        info!("list_available_orders failed {:?}", err);
        if !crate::restartDB() {
            return orders;
        }
        result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        let info = EngineOrder {
            id: row.get(0),
            price: row.get(1),
            amount: row.get(2),
            side: row.get(3),
            created_at: row.get(4),
        };
        orders.push(info);
    }
    orders
}
