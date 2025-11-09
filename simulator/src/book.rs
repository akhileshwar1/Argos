// src/book.rs
use crate::core::{DepthUpdate, Price, Qty};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

/// Keep bids keyed by price descending (we store as OrderedFloat but BTreeMap sorts ascending,
/// so we use negative key trick or use BTreeMap and iterate reverse).
#[derive(Clone, Debug)]
pub struct InMemoryBook {
    /// bids: price -> qty
    pub bids: BTreeMap<Price, Qty>,
    /// asks: price -> qty
    pub asks: BTreeMap<Price, Qty>,
    pub update_id: u64,
}

impl InMemoryBook {
    pub fn new() -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), update_id: 0 }
    }

    fn parse_price_qty(s: &[String;2]) -> (Price, Qty) {
        let p = s[0].parse::<f64>().unwrap_or(0.0);
        let q = s[1].parse::<f64>().unwrap_or(0.0);
        (OrderedFloat(p), q)
    }

    pub fn apply_update(&mut self, u: &DepthUpdate) {
        for lvl in &u.b {
            let (p,q) = Self::parse_price_qty(lvl);
            if q <= 0.0 {
                self.bids.remove(&p);
            } else {
                self.bids.insert(p, q);
            }
        }
        for lvl in &u.a {
            let (p,q) = Self::parse_price_qty(lvl);
            if q <= 0.0 {
                self.asks.remove(&p);
            } else {
                self.asks.insert(p, q);
            }
        }
        self.update_id = u.u;
    }

    pub fn load_snapshot(&mut self, bids: &Vec<(f64,f64)>, asks: &Vec<(f64,f64)>, seq_id: u64) {
        self.bids.clear();
        self.asks.clear();
        for (p,q) in bids {
            if *q > 0.0 {
                self.bids.insert(OrderedFloat(*p), *q);
            }
        }
        for (p,q) in asks {
            if *q > 0.0 {
                self.asks.insert(OrderedFloat(*p), *q);
            }
        }
        self.update_id = seq_id;
    }

    /// Get top of book: best bid (price, qty) and best ask (price, qty)
    pub fn top_of_book(&self) -> (Option<(f64,f64)>, Option<(f64,f64)>) {
        let best_bid = self.bids.iter().rev().next().map(|(p,q)| (p.into_inner(), *q));
        let best_ask = self.asks.iter().next().map(|(p,q)| (p.into_inner(), *q));
        (best_bid, best_ask)
    }

    /// Match a market buy: consume asks from lowest price upwards until qty satisfied. Returns (avg_price, qty_filled)
    pub fn match_market_buy(&mut self, mut qty: f64) -> (f64, f64) {
        let mut filled = 0.0;
        let mut cost = 0.0;
        // iterate asks ascending
        let mut to_delete = Vec::new();
        for (p, q) in self.asks.iter_mut() {
            if qty <= 0.0 { break; }
            let take = q.min(qty);
            *q -= take;
            qty -= take;
            filled += take;
            cost += take * p.into_inner();
            if *q <= 1e-12 { to_delete.push(*p); }
        }
        for p in to_delete { self.asks.remove(&p); }
        let avg_price = if filled > 0.0 { cost / filled } else { 0.0 };
        (avg_price, filled)
    }

    /// Match a market sell: consume bids descending
    pub fn match_market_sell(&mut self, mut qty: f64) -> (f64, f64) {
        let mut filled = 0.0;
        let mut proceeds = 0.0;
        let mut to_delete = Vec::new();
        for (p, q) in self.bids.iter_mut().rev() {
            if qty <= 0.0 { break; }
            let take = q.min(qty);
            *q -= take;
            qty -= take;
            filled += take;
            proceeds += take * p.into_inner();
            if *q <= 1e-12 { to_delete.push(*p); }
        }
        for p in to_delete { self.bids.remove(&p); }
        let avg_price = if filled > 0.0 { proceeds / filled } else { 0.0 };
        (avg_price, filled)
    }
}
