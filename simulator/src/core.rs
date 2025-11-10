// src/core.rs
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;

pub type Price = OrderedFloat<f64>;
pub type Qty = f64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub e: String,
    pub E: i64,
    pub s: String,
    pub U: u64,
    pub u: u64,
    pub b: Vec<[String; 2]>,
    pub a: Vec<[String; 2]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradeEvent {
    pub e: String,
    pub E: i64,
    pub s: String,
    pub t: u64,
    pub p: String,
    pub q: String,
    pub T: i64,
    pub m: bool,
    pub M: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookEvent {
    pub symbol: String,
    pub ts: DateTime<Utc>,
    pub best_bid_price: Option<f64>,
    pub best_bid_qty: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub best_ask_qty: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bar_15m {
    pub symbol: String,
    pub ts: DateTime<Utc>,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderIntent {
    pub id: String,
    pub symbol: String,
    pub side: String, // "buy" / "sell"
    pub price: Option<f64>, // limit or None for market
    pub qty: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: String,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MarketEvent {
    Depth(DepthUpdate),
    Trade(TradeEvent),
    Book(BookEvent),
    Bar_15m(Bar_15m),
}
