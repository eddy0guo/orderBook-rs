use postgres::{config::Config, error::Error, row::SimpleQueryRow, Client, NoTls};

extern crate rustc_serialize;

use rustc_serialize::json;

use serde::Deserialize;
use std::ops::Mul;

#[derive(Deserialize, Debug, Default)]
pub struct TokenInfo {
    symbol: String,
    name: String,
    address: String,
    decimals: i32,
}
#[derive(Deserialize,RustcDecodable, RustcEncodable, Debug, Default,Clone)]
pub struct EngineOrder {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: String,
    created_at: String
}
/*

 id               | text                        |          | not null |
 trade_hash       | text                        |          |          |
 transaction_id   | integer                     |          |          |
 transaction_hash | text                        |          |          |
 status           | text                        |          |          |
 market_id        | text                        |          |          |
 maker            | text                        |          |          |
 taker            | text                        |          |          |
 price            | numeric(32,8)               |          |          |
 amount           | numeric(32,8)               |          |          |
 taker_side       | text                        |          |          |
 maker_order_id   | text                        |          |          |
 taker_order_id   | text                        |          |          |
 updated_at       | timestamp without time zone |          |          |
 created_at       | timestamp without time zone |          |          |
*/
#[derive(Deserialize,RustcDecodable, RustcEncodable, Debug, Default,Clone)]
pub struct EngineTrade {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: String,
    created_at: String
}

#[derive(Deserialize, Debug, Default)]
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

#[derive(Deserialize, Debug, Default)]
pub struct MarketInfo {
    pub id: String,
    base_token_address: String,
    base_token_symbol: String,
    quote_token_address: String,
    quote_token_symbol: String,
}

#[derive(Deserialize, Debug, Default)]
pub struct MarketVolume {
    pub marketID: String,
    pub volume: f64,
}

use std::sync::Mutex;
use std::ptr::null;


pub fn get_change_rate(marketID: &str) -> f64 {
    let current_price = get_current_price_marketID(marketID);
    if current_price == 0.0 { return 0.0; }

    let mut yesterday_price = 0.0;
    let sql = "select cast(price as float8) from mist_trades_tmp where (current_timestamp - created_at) < '24 hours' and market_id=$1 order by created_at  limit 1";
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[&marketID]);
    if let Err(err) = result {
        println!("get_change_rate failed {:?}", err);
        if !crate::restartDB() {
            return 0.0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[&marketID]);
    }
    let rows = result.unwrap();
    for row in rows {
        yesterday_price = row.get(0);
    }
    if yesterday_price == 0.0 { return 0.0; }
    let rate = (current_price - yesterday_price) / yesterday_price;
    rate.mul(100000000.0).floor() / 100000000.0
}


pub fn get_current_price_marketID(id: &str) -> f64 {
    let sql = "select cast(price as float8) from mist_trades_tmp where (current_timestamp - created_at) < '24 hours' and market_id=$1 order by created_at desc limit 1";
    let mut price: f64 = 0.0;
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[&id]);

    if let Err(err) = result {
        println!("get_marketID_volume failed {:?}", err);
        if !crate::restartDB() {
            return 0.0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[&id]);
    }
    let rows = result.unwrap();
    for row in rows {
        price = row.get(0);
    }
    price
}

pub fn list_marketID_volume() -> Vec<MarketVolume> {
    let sql = "select market_id,cast(sum(amount) as float8) as volume  from mist_trades_tmp  where (current_timestamp - created_at) < '24 hours' group by market_id";
    let mut markets: Vec<MarketVolume> = Vec::new();
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    if let Err(err) = result {
        println!("get_marketID_volume failed {:?}", err);
        if !crate::restartDB() {
            return markets;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        let info = MarketVolume {
            marketID: row.get(0),
            volume: row.get(1),
        };
        markets.push(info);
    }

    markets
}

pub fn get_marketID_volume(id: &str) -> f64 {
    let sql = "select cast(sum(amount) as float8) as volume  from mist_trades_tmp  where (current_timestamp - created_at) < '24 hours' and market_id=$1";
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[&id]);
    if let Err(err) = result {
        println!("get_marketID_volume failed {:?}", err);
        if !crate::restartDB() {
            return 0.0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[&id]);
    }
    let rows = result.unwrap();
    let x: Option<f64> = rows[0].get(0);
    if x == None {
        println!("{} have no trade within 24 hours", id);
        return 0.00;
    }
    x.unwrap()
}

pub fn list_markets() -> Vec<MarketInfo> {
    let sql = "select id,base_token_address,base_token_symbol,quote_token_address,quote_token_symbol from mist_markets where online=true";
    let mut markets: Vec<MarketInfo> = Vec::new();
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    if let Err(err) = result {
        println!("get_active_address_num failed {:?}", err);
        if !crate::restartDB() {
            return markets;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        let info = MarketInfo {
            id: row.get(0),
            base_token_address: row.get(1),
            base_token_symbol: row.get(2),
            quote_token_address: row.get(3),
            quote_token_symbol: row.get(4),
        };
        markets.push(info);
    }
    markets
}


pub fn get_active_address_num() -> i32 {
    let sql = "select cast(count(1) as int) from (select taker from mist_trades_tmp where (current_timestamp - created_at) < '24 hours' group by taker)s";
    let mut num = 0;
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    if let Err(err) = result {
        println!("get_active_address_num failed {:?}", err);
        if !crate::restartDB() {
            return 0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        num = row.get(0);
    }
    num
}

pub fn get_trades_num() -> i32 {
    let sql = "select cast(count(1) as int) from mist_trades_tmp where (current_timestamp - created_at) < '24 hours'";
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    let mut num = 0;
    if let Err(err) = result {
        println!("get_trades_num failed {:?}", err);
        if !crate::restartDB() {
            return 0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        num = row.get(0);
    }
    num
}

pub fn get_new_address_num() -> i32 {
    let sql = "select cast(count(1) as int) from mist_users where (current_timestamp - created_at) < '24 hours'";
    let mut result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    let mut num = 0;
    if let Err(err) = result {
        println!("get_new_address_num failed {:?}", err);
        if !crate::restartDB() {
            return 0;
        }
        result = crate::CLIENTDB.lock().unwrap().query(sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        num = row.get(0);
    }
    num
}

pub fn list_available_orders(side: &str,channel: &str) -> Vec<EngineOrder> {
    let mut sort_by = "ASC";
    if side == "buy" {
        sort_by = "DESC";
    }
    let sql = format!( "select id,cast(price as float8),cast(available_amount as float8),side,cast(created_at as text) from mist_orders_tmp \
    where market_id='{}' and available_amount>0 and side='{}' order by price {} ,created_at ASC limit 10",channel,side,sort_by);
    println!("list_available_orders failed333 {}", sql);
    let mut orders: Vec<EngineOrder> = Vec::new();
    let mut result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    if let Err(err) = result {
        println!("get_active_address_num failed {:?}", err);
        if !crate::restartDB() {
            return orders;
        }
        result = crate::CLIENTDB.lock().unwrap().query(&*sql, &[]);
    }
    let rows = result.unwrap();
    for row in rows {
        let info = EngineOrder {
            id: row.get(0),
            price : row.get(1),
            amount: row.get(2),
            side: row.get(3),
            created_at: row.get(4),
        };
        orders.push(info);
    }
    println!("list_available_orders 444 {:?}------44", orders);
    orders
}
