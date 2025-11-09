// src/replayer.rs
use crate::core::*;
use crate::book::InMemoryBook;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Duration};

/// Replayer that reads snapshot + depth_raw + trades from Postgres and emits events.
/// If DATABASE_URL not present, falls back to synthetic generator.
pub struct Replayer {
    pub evt_tx: Sender<MarketEvent>,
    pub pool: Option<PgPool>,
    pub speed_factor: f64, // e.g., 1.0 for real-time, >1 faster
}

impl Replayer {
    pub fn new(evt_tx: Sender<MarketEvent>, pool: Option<PgPool>, speed_factor: f64) -> Self {
        Self { evt_tx, pool, speed_factor }
    }

    /// run historical playback for a given symbol and time range (inclusive)
    pub async fn run_db_playback(self: Arc<Self>, symbol: &str, start_ts: DateTime<Utc>, end_ts: DateTime<Utc>) -> Result<()> {
        let pool = match &self.pool {
            Some(p) => p.clone(),
            None => {
                // fallback to synthetic loop
                return self.run_synth().await;
            }
        };

        // 1) find last snapshot at or before start_ts
        let snap_row = sqlx::query!(
            r#"
            SELECT snapshot_time, seq_id, bids, asks
            FROM depth_snapshots
            WHERE symbol = $1 AND snapshot_time <= $2
            ORDER BY snapshot_time DESC
            LIMIT 1
            "#,
            symbol,
            start_ts
        ).fetch_optional(&pool).await?;

        let mut book = InMemoryBook::new();

        if let Some(sr) = snap_row {
            // bids/asks stored as JSON arrays of [price, qty]
            let bids: Vec<(f64,f64)> = serde_json::from_value(sr.bids.unwrap_or_else(|| serde_json::json!([])))?;
            let asks: Vec<(f64,f64)> = serde_json::from_value(sr.asks.unwrap_or_else(|| serde_json::json!([])))?;
            let seq_id = sr.seq_id.unwrap_or(0) as u64;
            book.load_snapshot(&bids, &asks, seq_id);
        } else {
            // no snapshot; start with empty book
        }

        // 2) load depth_raw events and trades after snapshot_time (or start_ts) ordered by event_time
        let depth_rows = sqlx::query!(
            r#" SELECT event_time, raw
            FROM depth_raw
            WHERE symbol = $1 AND event_time >= $2 AND event_time <= $3
            ORDER BY event_time ASC
            "#,
            symbol,
            start_ts,
            end_ts
        ).fetch_all(&pool).await?;

        let trade_rows = sqlx::query!(
            r#" SELECT event_time, raw
            FROM trades
            WHERE symbol = $1 AND event_time >= $2 AND event_time <= $3
            ORDER BY event_time ASC
            "#,
            symbol,
            start_ts,
            end_ts
        ).fetch_all(&pool).await?;

        // parse into unified timeline
        #[derive(Clone)]
        enum Row {
            Depth(chrono::DateTime<Utc>, DepthUpdate),
            Trade(chrono::DateTime<Utc>, TradeEvent),
        }

        let mut rows: Vec<Row> = Vec::new();
        for r in depth_rows {
            let ts: chrono::DateTime<Utc> = r.event_time;
            let du: DepthUpdate = serde_json::from_value(r.raw)?;
            rows.push(Row::Depth(ts, du));
        }
        for r in trade_rows {
            let ts: chrono::DateTime<Utc> = r.event_time;
            let te: TradeEvent = serde_json::from_value(r.raw.expect("REASON"))?;
            rows.push(Row::Trade(ts, te));
        }

        // sort by timestamp
        rows.sort_by_key(|r| match r {
            Row::Depth(t, _) => *t,
            Row::Trade(t, _) => *t,
        });

        if rows.is_empty() {
            // nothing to play; return
            return Ok(());
        }

        // virtual clock: map source timestamps to wall clock using speed factor
        let src_start = match &rows[0] {
            Row::Depth(t, _) => *t,
            Row::Trade(t, _) => *t,
        };
        let wall_start = tokio::time::Instant::now();

        for r in rows.into_iter() {
            let src_ts = match &r {
                Row::Depth(t, _) => *t,
                Row::Trade(t, _) => *t,
            };
            let src_delta = src_ts.signed_duration_since(src_start);
            let wait_wall = wall_start + Duration::from_secs_f64(src_delta.num_milliseconds() as f64 / 1000.0 / self.speed_factor);
            let now = tokio::time::Instant::now();
            if wait_wall > now {
                tokio::time::sleep(wait_wall - now).await;
            }

            match r {
                Row::Depth(_, du) => {
                    // apply to book
                    book.apply_update(&du);
                    // publish book event and depth event
                    let (bid, ask) = book.top_of_book();
                    let be = BookEvent {
                        symbol: du.s.clone(),
                        ts: chrono::Utc::now(),
                        best_bid_price: bid.map(|(p, _)| p),
                        best_bid_qty: bid.map(|(_, q)| q),
                        best_ask_price: ask.map(|(p, _)| p),
                        best_ask_qty: ask.map(|(_, q)| q),
                    };
                    let _ = self.evt_tx.send(MarketEvent::Depth(du.clone()));
                    let _ = self.evt_tx.send(MarketEvent::Book(be));
                }
                Row::Trade(_, te) => {
                    let _ = self.evt_tx.send(MarketEvent::Trade(te.clone()));
                    // bar building for 1m could be produced here; for brevity, strategy receives trades and can build or we can add bar builder as needed.
                }
            }
        }

        Ok(())
    }

    /// Simple synthetic fallback: generate trades and depth updates so you can run UI without DB
    pub async fn run_synth(&self) -> Result<()> {
        let mut seq: u64 = 1;
        let mut last_price = 10000.0;
        loop {
            tokio::time::sleep(Duration::from_millis(800)).await;
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
            let _ = self.evt_tx.send(MarketEvent::Trade(tr));
            // small synthetic depth update: move tiny qty on top
            let du = DepthUpdate {
                e: "depthUpdate".to_string(),
                E: now_ms,
                s: "BTCUSDT".to_string(),
                U: seq as u64,
                u: (seq + 1) as u64,
                b: vec![[format!("{:.2}", last_price - 0.5), format!("{:.6}", 0.1)]],
                a: vec![[format!("{:.2}", last_price + 0.5), format!("{:.6}", 0.1)]],
            };
            let _ = self.evt_tx.send(MarketEvent::Depth(du));
        }
    }
}

