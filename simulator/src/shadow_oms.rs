// src/shadow_oms.rs
use crate::core::*;
use anyhow::Result;
use chrono::Utc;
use tokio::sync::{mpsc::Receiver, broadcast::Sender};

/// Shadow OMS: consumes OrderIntent and simulates immediate fills at last trade price.
/// In a real system, this should check the in-memory book; here we simplify.
pub struct ShadowOms {
    pub order_rx: Receiver<OrderIntent>,
    pub fill_tx: Sender<Fill>,
    // keep last trade price for fill simulation
    pub last_trade_price: std::sync::Arc<std::sync::Mutex<f64>>,
}

impl ShadowOms {
    pub fn new(order_rx: Receiver<OrderIntent>, fill_tx: Sender<Fill>) -> Self {
        Self {
            order_rx,
            fill_tx,
            last_trade_price: std::sync::Arc::new(std::sync::Mutex::new(10000.0)),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(order) = self.order_rx.recv().await {
            // use last trade price
            let p = { *self.last_trade_price.lock().unwrap() };
            let fill = Fill {
                order_id: order.id.clone(),
                symbol: order.symbol.clone(),
                price: p,
                qty: order.qty,
                ts: chrono::Utc::now(),
            };
            // persist to DB is omitted here; send via channel
            let _ = self.fill_tx.send(fill);
        }
        Ok(())
    }
}
