// src/main.rs
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use std::time::Duration as StdDuration;

mod core;
mod book;
mod replayer;
mod strategy;
mod shadow_oms;
mod tui;

use crate::core::{MarketEvent, OrderIntent, Fill};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    // short, focused CLI via env
    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let speed_factor: f64 = env::var("SPEED_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(10.0);

    // optional DB
    let pool = match std::env::var("DATABASE_URL") {
        Ok(dburl) => {
            let p = PgPool::connect(&dburl).await?;
            Some(p)
        }
        Err(_) => None,
    };

    // channels
    // market events (broadcast): replayer -> many (strategy, shadow oms, tui)
    let (evt_tx, _evt_rx) = broadcast::channel::<MarketEvent>(2048);

    // orders: strategy -> shadow OMS (single consumer)
    let (order_tx, order_rx) = mpsc::channel::<OrderIntent>(256);

    // order broadcast for TUI / logger
    let (order_pub, _order_sub) = broadcast::channel::<OrderIntent>(1024);

    // fills: shadow OMS -> strategy + tui
    let (fill_tx, _fill_sub) = broadcast::channel::<Fill>(1024);

    // spawn Shadow OMS (non-blocking)
    {
        // take ownership of the receivers/senderrclones we need
        let order_rx = order_rx;               // mpsc::Receiver<OrderIntent> (moved into task)
        let fill_tx = fill_tx.clone();         // Sender<Fill>
        let depth_rx = evt_tx.subscribe();     // broadcast::Receiver<MarketEvent>
        if let Some(pool_clone) = pool.clone() {
            // debug log so you see we attempted spawn
            eprintln!("Spawning ShadowOms task");
            tokio::spawn(async move {
                let oms = shadow_oms::ShadowOms::new(order_rx, fill_tx, depth_rx, pool_clone);
                if let Err(e) = oms.run().await {
                    eprintln!("ShadowOms exited with err: {}", e);
                } else {
                    eprintln!("ShadowOms exited normally");
                }
            });
        } else {
            eprintln!("No DATABASE_URL found — ShadowOms disabled (requires DB).");
        }
    }

    // spawn Strategy
    {
        let evt_rx = evt_tx.subscribe();
        let order_tx_clone = order_tx.clone();
        let order_pub_clone = order_pub.clone();
        let fill_rx = fill_tx.subscribe();
        let strat_task = tokio::spawn(async move {
            let strat = strategy::Strategy::new(evt_rx, order_tx_clone, order_pub_clone, fill_rx);
            if let Err(e) = strat.run().await {
                eprintln!("Strategy exited with err: {}", e);
            }
        });
        let _ = strat_task;
    }

    // spawn TUI
    {
        let evt_rx = evt_tx.subscribe();
        let fill_rx = fill_tx.subscribe();
        let order_rx_for_tui = order_pub.subscribe();
        let tui_task = tokio::spawn(async move {
            if let Err(e) = tui::run_tui(evt_rx, fill_rx, order_rx_for_tui).await {
                eprintln!("TUI exited with err: {}", e);
            }
        });
        let _ = tui_task;
    }

    // start replayer: if DB present, use db playback; else synth
    {
        let evt_tx_clone = evt_tx.clone();
        match pool {
            Some(p) => {
                // create replayer with DB pool
                let rep = replayer::Replayer::new(evt_tx_clone, Some(p.clone()), speed_factor);
                let rep_arc = Arc::new(rep);

                // pick playback interval: env START_TS/END_TS as RFC3339 or default last 24h
                let end_ts_now: DateTime<Utc> = Utc::now();
                let start_ts = match env::var("START_RFC3339") {
                    Ok(s) => DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&Utc)).unwrap_or(end_ts_now - Duration::hours(2)),
                    Err(_) => end_ts_now - Duration::hours(3),
                };
                let end_ts = match env::var("END_RFC3339") {
                    Ok(s) => DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&Utc)).unwrap_or(end_ts_now),
                    Err(_) => end_ts_now,
                };

                let symbol_clone = symbol.clone();
                let rep_handle = {
                    let rep_arc = rep_arc.clone();
                    tokio::spawn(async move {
                        if let Err(e) = rep_arc.run_db_playback(&symbol_clone, start_ts, end_ts).await {
                            eprintln!("Replayer DB playback failed: {}", e);
                        }
                    })
                };
                let _ = rep_handle;
            }
            None => {
                // no DB: spawn replayer synthetic run (non-blocking)
                let rep = replayer::Replayer::new(evt_tx_clone, None, speed_factor);
                let rep_task = tokio::spawn(async move {
                    if let Err(e) = rep.run_synth().await {
                        eprintln!("Replayer synth failed: {}", e);
                    }
                });
                let _ = rep_task;
            }
        }
    }

    // keep main alive until ctrl-c
    tokio::signal::ctrl_c().await?;
    eprintln!("Shutdown requested — exiting.");
    // small delay to let tasks flush DB or output
    tokio::time::sleep(StdDuration::from_millis(200)).await;
    Ok(())
}
