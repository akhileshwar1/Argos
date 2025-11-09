// src/strategy.rs
use crate::core::*;
use anyhow::Result;
use chrono::Utc;
use tokio::sync::{broadcast, mpsc, broadcast::Receiver};
use uuid::Uuid;

pub struct Strategy {
    pub evt_rx: Receiver<MarketEvent>,
    pub order_tx: mpsc::Sender<OrderIntent>,                // to OMS (single consumer)
    pub order_pub: broadcast::Sender<OrderIntent>,          // broadcast so TUI can show orders
    pub fill_rx: broadcast::Receiver<Fill>,
}

impl Strategy {
    pub fn new(
        evt_rx: Receiver<MarketEvent>,
        order_tx: mpsc::Sender<OrderIntent>,
        order_pub: broadcast::Sender<OrderIntent>,
        fill_rx: broadcast::Receiver<Fill>,
    ) -> Self {
        Self { evt_rx, order_tx, order_pub, fill_rx }
    }

    pub async fn run(mut self) -> Result<()> {
        // simple 1m bar builder from trades
        let mut open: Option<f64> = None;
        let mut high: f64 = f64::MIN;
        let mut low: f64 = f64::MAX;
        let mut close: f64 = 0.0;
        let mut vol: f64 = 0.0;
        let mut current_bucket: Option<i64> = None;

        loop {
            tokio::select! {
                ev = self.evt_rx.recv() => {
                    match ev {
                        Ok(MarketEvent::Trade(tr)) => {
                            // convert trade timestamp safely
                            let ts_opt = chrono::NaiveDateTime::from_timestamp_opt(tr.T / 1000, ((tr.T % 1000) as u32) * 1_000_000);
                            let ts = ts_opt
                                .map(|n| chrono::DateTime::<Utc>::from_utc(n, Utc))
                                .unwrap_or_else(|| Utc::now());
                            let epoch = ts.timestamp();
                            let bucket = epoch - (epoch % 60);
                            let price = tr.p.parse::<f64>().unwrap_or(0.0);
                            let qty = tr.q.parse::<f64>().unwrap_or(0.0);

                            if current_bucket.is_none() {
                                current_bucket = Some(bucket);
                                open = Some(price);
                                high = price;
                                low = price;
                                close = price;
                                vol = qty;
                            } else if current_bucket.unwrap() != bucket {
                                // flush bar
                                let bar = Bar {
                                    symbol: tr.s.clone(),
                                    tf: "60s".to_string(),
                                    ts: chrono::DateTime::<Utc>::from_utc(chrono::NaiveDateTime::from_timestamp_opt(current_bucket.unwrap(), 0).unwrap(), Utc),
                                    o: open.unwrap_or(price),
                                    h: high,
                                    l: low,
                                    c: close,
                                    v: vol,
                                };

                                // simple signal: buy on up-bar, sell on down-bar
                                let side = if bar.c > bar.o { "buy" } else { "sell" };
                                let order = OrderIntent {
                                    id: Uuid::new_v4().to_string(),
                                    symbol: bar.symbol.clone(),
                                    side: side.to_string(),
                                    price: None,
                                    qty: 0.0005,
                                    ts: Utc::now(),
                                };

                                // send to OMS (mpsc)
                                let _ = self.order_tx.send(order.clone()).await;

                                // publish to broadcast so UI and other listeners can see it
                                let _ = self.order_pub.send(order.clone());

                                // start new bucket
                                current_bucket = Some(bucket);
                                open = Some(price);
                                high = price;
                                low = price;
                                close = price;
                                vol = qty;
                            } else {
                                // accumulate
                                if price > high { high = price; }
                                if price < low { low = price; }
                                close = price;
                                vol += qty;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {
                            // publisher closed -> exit gracefully
                            return Ok(());
                        }
                    }
                }
                fill = self.fill_rx.recv() => {
                    match fill {
                        Ok(f) => {
                            // receive fills; we could keep position state
                            // (left minimal)
                            println!("Strategy received fill: {:?}", f);
                        }
                        Err(_) => {}
                    }
                }
            }
        }
    }
}
