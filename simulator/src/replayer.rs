// src/replayer.rs
use crate::core::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Duration};

#[cfg(feature = "sqlx")]
use sqlx::PgPool;

/// Small in-memory aggregator to produce 1m bars from trades.
struct BarBuilder {
    tf_secs: i64,
    current_open: Option<f64>,
    current_high: f64,
    current_low: f64,
    current_close: f64,
    current_vol: f64,
    current_ts: Option<DateTime<Utc>>,
}

impl BarBuilder {
    fn new(tf_secs: i64) -> Self {
        Self {
            tf_secs,
            current_open: None,
            current_high: f64::MIN,
            current_low: f64::MAX,
            current_close: 0.0,
            current_vol: 0.0,
            current_ts: None,
        }
    }

    fn on_trade(&mut self, t: &TradeEvent) -> Option<Bar> {
        let p = t.p.parse::<f64>().unwrap_or(0.0);
        let q = t.q.parse::<f64>().unwrap_or(0.0);
        let dt = DateTime::<Utc>::from_utc(
            chrono::NaiveDateTime::from_timestamp_opt(t.T / 1000, ((t.T % 1000) as u32) * 1_000_000).unwrap_or_else(|| chrono::NaiveDateTime::from_timestamp(0, 0)),
            Utc,
        );

        // floor ts to tf_secs
        let epoch = dt.timestamp();
        let bucket = epoch - (epoch % self.tf_secs);
        let bucket_dt = DateTime::<Utc>::from_utc(chrono::NaiveDateTime::from_timestamp_opt(bucket, 0).unwrap(), Utc);

        if self.current_ts.is_none() {
            self.current_ts = Some(bucket_dt);
            self.current_open = Some(p);
            self.current_high = p;
            self.current_low = p;
            self.current_close = p;
            self.current_vol = q;
            return None;
        }

        if Some(bucket_dt) != self.current_ts {
            // flush old bar
            let bar = Bar {
                symbol: t.s.clone(),
                tf: format!("{}s", self.tf_secs),
                ts: self.current_ts.unwrap(),
                o: self.current_open.unwrap_or(p),
                h: self.current_high,
                l: self.current_low,
                c: self.current_close,
                v: self.current_vol,
            };
            // start new
            self.current_ts = Some(bucket_dt);
            self.current_open = Some(p);
            self.current_high = p;
            self.current_low = p;
            self.current_close = p;
            self.current_vol = q;
            return Some(bar);
        } else {
            // accumulate
            if p > self.current_high { self.current_high = p; }
            if p < self.current_low { self.current_low = p; }
            self.current_close = p;
            self.current_vol += q;
            return None;
        }
    }
}

/// Replayer config: either read DB (if pool provided) or generate synthetic trades from CSV/local generator.
pub struct Replayer {
    pub evt_tx: Sender<MarketEvent>,
    pub trade_buffer: Arc<Mutex<VecDeque<TradeEvent>>>,
    #[cfg(feature = "sqlx")]
    pub pool: Option<PgPool>,
}

impl Replayer {
    pub fn new(evt_tx: Sender<MarketEvent>) -> Self {
        Self {
            evt_tx,
            trade_buffer: Arc::new(Mutex::new(VecDeque::new())),
            #[cfg(feature = "sqlx")]
            pool: None,
        }
    }

    /// Start replayer loop (spawned in main).
    /// If DATABASE_URL is present and sqlx feature is enabled, will attempt DB playback; otherwise synthetic.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        // spawn a small synthetic trade generator if DB not available
        // Also build bars (1m) and publish MarketEvent::Bar when bars close.
        let tb = self.trade_buffer.clone();
        let evt = self.evt_tx.clone();

        // bar builder for 1m (60s)
        let mut bar_builder = BarBuilder::new(60);

        // If the environment variable DATABASE_URL exists and sqlx feature enabled, you could query trades here.
        // For now, we'll run a synthetic generator (random walk) to produce trades every second.
        tokio::spawn(async move {
            let mut seq: u64 = 1;
            let mut last_price = 10000.0f64;
            loop {
                // produce one trade per second
                tokio::time::sleep(Duration::from_millis(800)).await;
                // random-ish walk
                let change = (rand::random::<f64>() - 0.5) * 5.0;
                last_price = (last_price + change).max(0.1);
                let qty = (rand::random::<f64>() * 0.02).max(0.0001);

                let now_ms = chrono::Utc::now().timestamp_millis();
                let tr = TradeEvent {
                    e: "trade".to_string(),
                    E: now_ms,
                    s: "BTCUSDT".to_string(),
                    t: seq,
                    p: format!("{:.2}", last_price),
                    q: format!("{:.6}", qty),
                    T: now_ms,
                    m: rand::random::<bool>(),
                    M: true,
                };
                seq += 1;

                // push to buffer and publish the trade event
                {
                    let mut q = tb.lock().unwrap();
                    q.push_back(tr.clone());
                }
                let _ = evt.send(MarketEvent::Trade(tr.clone()));

                // bar builder
                if let Some(bar) = bar_builder.on_trade(&tr) {
                    let _ = evt.send(MarketEvent::Bar(bar));
                }
            }
        });

        Ok(())
    }
}
