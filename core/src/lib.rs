use serde::{Serialize,Deserialize};
use chrono::{DateTime, Utc};

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Trade { pub ts: DateTime<Utc>, pub price: f64, pub qty: f64, pub side: String }

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct DepthSnapshot { pub ts: DateTime<Utc>, pub bids: Vec<(f64,f64)>, pub asks: Vec<(f64,f64)> }

#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum MarketEvent { Trade(Trade), Snapshot(DepthSnapshot), DepthUpdate(String) }

pub type Event = MarketEvent;
