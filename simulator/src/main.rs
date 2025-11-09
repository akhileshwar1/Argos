// src/main.rs
mod core;
mod replayer;
mod strategy;
mod shadow_oms;
mod tui;
mod book;

use crate::core::*;
use anyhow::Result;
use dotenvy::dotenv;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use chrono::{Utc, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // channels
    let (evt_tx, _evt_rx) = broadcast::channel::<MarketEvent>(4096);
    let (order_tx, order_rx) = mpsc::channel::<OrderIntent>(1024);
    let (order_pub, _order_sub) = broadcast::channel::<OrderIntent>(1024); // broadcast for TUI and others
    let (fill_tx, _fill_rx) = broadcast::channel::<Fill>(1024);

    // If DATABASE_URL present, create sqlx pool
    let db_url = std::env::var("DATABASE_URL").ok();
    println!("DB URL IS {:?}", db_url);
    let pool = if let Some(url) = db_url {
        Some(sqlx::PgPool::connect(&url).await?)
    } else { None };

    // REPLAYER: if pool exists, run DB playback for last N minutes; otherwise synth
    let replayer = Arc::new(replayer::Replayer::new(evt_tx.clone(), pool.clone(), 20.0));
    let rep = replayer.clone();
    if pool.is_some() {
        // run playback for the last 60 minutes
        let end = Utc::now();
        let start = end - Duration::minutes(180);
        let s = rep.clone();
        tokio::spawn(async move {
            if let Err(e) = s.run_db_playback("BTCUSDT", start, end).await {
                eprintln!("replayer db playback error: {:?}", e);
            }
        });
    } else {
        let s = rep.clone();
        tokio::spawn(async move {
            if let Err(e) = s.run_synth().await {
                eprintln!("replayer synth error: {:?}", e);
            }
        });
    }

    // Shadow OMS needs depth to update book; create a subscriber to events
    let depth_sub = evt_tx.subscribe();
    let oms = shadow_oms::ShadowOms::new(order_rx, fill_tx.clone(), depth_sub);
    tokio::spawn(async move {
        if let Err(e) = oms.run().await { eprintln!("oms error: {:?}", e); }
    });

    // Strategy subscribes to events and fills; give it both order_tx (mpsc) and order_pub (broadcast)
    let strat = strategy::Strategy::new(evt_tx.subscribe(), order_tx.clone(), order_pub.clone(), fill_tx.subscribe());
    tokio::spawn(async move {
        if let Err(e) = strat.run().await { eprintln!("strategy error: {:?}", e); }
    });

    // TUI: subscribe to events/fills and orders broadcast
    let tui_evt = evt_tx.subscribe();
    let tui_fill = fill_tx.subscribe();
    let tui_orders = order_pub.subscribe();

    // Run tui
    tokio::spawn(async move {
        if let Err(e) = tui::run_tui(tui_evt, tui_fill, tui_orders).await {
            eprintln!("tui error: {:?}", e);
        }
    });

    // keep alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
