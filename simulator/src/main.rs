// src/main.rs
mod core;
mod replayer;
mod strategy;
mod shadow_oms;
mod tui;

use crate::core::*;
use anyhow::Result;
use dotenvy::dotenv;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // channels:
    // events: replayer -> many (strategy + tui)
    let (evt_tx, _evt_rx) = broadcast::channel::<MarketEvent>(4096);
    // order intents: strategy -> oms (single consumer)
    let (order_tx, order_rx) = mpsc::channel::<OrderIntent>(1024);
    // fills: oms -> many (strategy + tui)
    let (fill_tx, _fill_rx) = broadcast::channel::<Fill>(1024);

    // spawn replayer
    let replayer = Arc::new(replayer::Replayer::new(evt_tx.clone()));
    let rep = replayer.clone();
    tokio::spawn(async move {
        if let Err(e) = rep.run().await {
            eprintln!("replayer error: {:?}", e);
        }
    });

    // spawn strategy
    let strat = strategy::Strategy::new(evt_tx.subscribe(), order_tx.clone(), fill_tx.subscribe());
    tokio::spawn(async move {
        if let Err(e) = strat.run().await {
            eprintln!("strategy error: {:?}", e);
        }
    });

    // spawn OMS
    let oms = shadow_oms::ShadowOms::new(order_rx, fill_tx.clone());
    tokio::spawn(async move {
        if let Err(e) = oms.run().await {
            eprintln!("oms error: {:?}", e);
        }
    });

    // spawn TUI (subscribes to events and fills)
    let tui_evt_rx = evt_tx.subscribe();
    let tui_fill_rx = fill_tx.subscribe();
    tokio::spawn(async move {
        tui::run_tui(tui_evt_rx, tui_fill_rx).await;
    });

    // keep main alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
