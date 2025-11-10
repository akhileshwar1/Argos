// src/replayer.rs
use crate::core::{Bar_15m, MarketEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Duration};

/// Replayer that emits only 15m bars from DB (or a lightweight synthetic fallback).
pub struct Replayer {
    pub evt_tx: Sender<MarketEvent>,
    pub pool: Option<PgPool>,
    pub speed_factor: f64, // 1.0 = realtime, >1 = faster
}

impl Replayer {
    pub fn new(evt_tx: Sender<MarketEvent>, pool: Option<PgPool>, speed_factor: f64) -> Self {
        Self { evt_tx, pool, speed_factor }
    }

    /// Play back 15m bars for symbol between start_ts and end_ts (inclusive).
    pub async fn run_db_playback(self: Arc<Self>, symbol: &str, start_ts: DateTime<Utc>, end_ts: DateTime<Utc>) -> Result<()> {
        let pool = match &self.pool {
            Some(p) => p.clone(),
            None => return self.run_synth().await,
        };

        // fetch bars from DB
        let rows = sqlx::query!(
            r#"
            SELECT ts, open, high, low, close, volume
            FROM bars_15m
            WHERE symbol = $1 AND ts >= $2 AND ts <= $3
            ORDER BY ts ASC
            "#,
            symbol,
            start_ts,
            end_ts
        )
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            return Ok(());
        }

        // virtual clock mapping
        let src_start = rows[0].ts;
        let wall_start = tokio::time::Instant::now();

        for r in rows.into_iter() {
            let src_ts: DateTime<Utc> = r.ts;
            let delta_ms = (src_ts.signed_duration_since(src_start).num_milliseconds() as f64) / self.speed_factor;
            let wait_wall = wall_start + Duration::from_millis(delta_ms as u64);

            let now = tokio::time::Instant::now();
            if wait_wall > now {
                sleep(wait_wall - now).await;
            }

            // build Bar (core::Bar expected)
            let bar = Bar_15m {
                symbol: symbol.to_string(),
                ts: src_ts,
                o: r.open,
                h: r.high,
                l: r.low,
                c: r.close,
                v: r.volume,
            };

            let _ = self.evt_tx.send(MarketEvent::Bar_15m(bar));
        }

        Ok(())
    }

    /// Lightweight synthetic fallback that emits dummy bars so UI/strategy can run without DB.
    pub async fn run_synth(&self) -> Result<()> {
        let mut price = 50_000.0;
        loop {
            // create synthetic 15-min bar every 500ms wall time (fast)
            let o = price;
            let h = o + (rand::random::<f64>() * 50.0);
            let l = o - (rand::random::<f64>() * 50.0);
            let c = l + rand::random::<f64>() * (h - l);
            let v = 1.0 + rand::random::<f64>() * 10.0;
            let bar = Bar_15m {
                symbol: "BTCUSDT".to_string(),
                ts: chrono::Utc::now(),
                o,
                h,
                l,
                c,
                v,
            };
            let _ = self.evt_tx.send(MarketEvent::Bar_15m(bar));
            price = c;
            sleep(Duration::from_millis(500)).await;
        }
    }
}
