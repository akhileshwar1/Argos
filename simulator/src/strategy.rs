// src/strategy.rs
use crate::core::*;
use anyhow::Result;
use chrono::Utc;
use tokio::sync::{broadcast, broadcast::Receiver, mpsc::Sender};
use uuid::Uuid;

pub struct Strategy {
    pub evt_rx: Receiver<MarketEvent>,
    pub order_tx: Sender<OrderIntent>,
    pub fill_rx: broadcast::Receiver<Fill>,
}

impl Strategy {
    pub fn new(evt_rx: Receiver<MarketEvent>, order_tx: Sender<OrderIntent>, fill_rx: broadcast::Receiver<Fill>) -> Self {
        Self { evt_rx, order_tx, fill_rx }
    }

    pub async fn run(mut self) -> Result<()> {
        // Example strategy: on every bar, if close increased vs open -> buy small; else sell small.
        loop {
            tokio::select! {
                res = self.evt_rx.recv() => {
                    match res {
                        Ok(MarketEvent::Bar(bar)) => {
                            // simple signal
                            let side = if bar.c > bar.o { "buy" } else { "sell" };
                            let qty = 0.0005; // tiny
                            let order = OrderIntent {
                                id: Uuid::new_v4().to_string(),
                                symbol: bar.symbol.clone(),
                                side: side.to_string(),
                                price: None, // market
                                qty,
                                ts: chrono::Utc::now(),
                            };
                            // send to OMS
                            let _ = self.order_tx.send(order).await;
                        }
                        Ok(_other) => {
                            // ignore
                        }
                        Err(_) => {
                            // publisher closed
                            break;
                        }
                    }
                }
                // optional: handle fills (e.g., update state)
                fill_res = self.fill_rx.recv() => {
                    match fill_res {
                        Ok(fill) => {
                            // naive print
                            println!("Strategy observed fill: {:?} ", fill);
                        }
                        Err(_) => { /* ignore */ }
                    }
                }
            }
        }
        Ok(())
    }
}
