// src/shadow_oms.rs
use crate::core::*;
use crate::book::InMemoryBook;
use anyhow::Result;
use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast::Receiver, mpsc::Receiver as MReceiver, broadcast::Sender};

pub struct ShadowOms {
    pub order_rx: MReceiver<OrderIntent>,
    pub fill_tx: Sender<Fill>,
    pub depth_rx: Receiver<MarketEvent>, // to receive Depth events to update book
    pub book: Arc<Mutex<InMemoryBook>>,
}

impl ShadowOms {
    pub fn new(order_rx: MReceiver<OrderIntent>, fill_tx: Sender<Fill>, depth_rx: Receiver<MarketEvent>) -> Self {
        Self {
            order_rx,
            fill_tx,
            depth_rx,
            book: Arc::new(Mutex::new(InMemoryBook::new())),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                // handle incoming orders
                ord = self.order_rx.recv() => {
                    match ord {
                        Some(o) => {
                            // market orders only for now
                            if o.side == "buy" {
                                let (avg, filled) = {
                                    let mut b = self.book.lock().unwrap();
                                    b.match_market_buy(o.qty)
                                };
                                if filled > 0.0 {
                                    let fill = Fill {
                                        order_id: o.id.clone(),
                                        symbol: o.symbol.clone(),
                                        price: avg,
                                        qty: filled,
                                        ts: Utc::now(),
                                    };
                                    let _ = self.fill_tx.send(fill);
                                } else {
                                    // no liquidity: return zero fill (or queued)
                                    let fill = Fill {
                                        order_id: o.id.clone(),
                                        symbol: o.symbol.clone(),
                                        price: 0.0,
                                        qty: 0.0,
                                        ts: Utc::now(),
                                    };
                                    let _ = self.fill_tx.send(fill);
                                }
                            } else {
                                let (avg, filled) = {
                                    let mut b = self.book.lock().unwrap();
                                    b.match_market_sell(o.qty)
                                };
                                if filled > 0.0 {
                                    let fill = Fill {
                                        order_id: o.id.clone(),
                                        symbol: o.symbol.clone(),
                                        price: avg,
                                        qty: filled,
                                        ts: Utc::now(),
                                    };
                                    let _ = self.fill_tx.send(fill);
                                } else {
                                    let fill = Fill {
                                        order_id: o.id.clone(),
                                        symbol: o.symbol.clone(),
                                        price: 0.0,
                                        qty: 0.0,
                                        ts: Utc::now(),
                                    };
                                    let _ = self.fill_tx.send(fill);
                                }
                            }
                        }
                        None => {
                            // channel closed
                            break;
                        }
                    }
                }

                // handle depth updates to maintain book
                ev = self.depth_rx.recv() => {
                    match ev {
                        Ok(Me) => {
                            match Me {
                                MarketEvent::Depth(du) => {
                                    let mut b = self.book.lock().unwrap();
                                    b.apply_update(&du);
                                }
                                _ => {}
                            }
                        }
                        Err(_) => { break; }
                    }
                }
            }
        }
        Ok(())
    }
}
