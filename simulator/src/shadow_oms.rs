// src/shadow_oms.rs
use crate::core::*;
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures_util::stream::TryStreamExt;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast::Receiver, broadcast::Sender, mpsc::Receiver as MReceiver};

const FILL_WINDOW_SECS: i64 = 300; // 5 minutes
const FEE_RATE: f64 = 0.001;       // 0.1%

#[derive(Debug, Clone)]
struct PositionState {
    net_qty: f64,
    avg_price: f64,
    realized_pnl: f64,
    total_fees: f64,
}

impl PositionState {
    fn new() -> Self {
        Self { net_qty: 0.0, avg_price: 0.0, realized_pnl: 0.0, total_fees: 0.0 }
    }
}

pub struct ShadowOms {
    pub order_rx: MReceiver<OrderIntent>,
    pub fill_tx: Sender<Fill>,
    pub depth_rx: Receiver<MarketEvent>,
    pub pool: PgPool,
    positions: Arc<Mutex<HashMap<String, PositionState>>>,
}

impl ShadowOms {
    pub fn new(order_rx: MReceiver<OrderIntent>, fill_tx: Sender<Fill>, depth_rx: Receiver<MarketEvent>, pool: PgPool) -> Self {
        Self {
            order_rx,
            fill_tx,
            depth_rx,
            pool,
            positions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn persist_order(&self, o: &OrderIntent) -> Result<()> {
        let raw = serde_json::to_value(o).unwrap_or_else(|_| serde_json::json!({}));
        let price_hint = o.price;
        sqlx::query(
            "INSERT INTO orders(id, symbol, side, qty, price_hint, ts, raw, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
        )
            .bind(&o.id)
            .bind(&o.symbol)
            .bind(&o.side)
            .bind(o.qty)
            .bind(price_hint)
            .bind(o.ts)
            .bind(raw)
            .bind("new")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn persist_fill_db(&self, f: &Fill) -> Result<()> {
        let raw = serde_json::to_value(f).unwrap_or_else(|_| serde_json::json!({}));
        let fee = (f.price * f.qty) * FEE_RATE;
        // NOTE: side column is currently placeholder; you can change schema to store side explicitly.
        sqlx::query(
            "INSERT INTO fills(order_id, symbol, side, qty, price, fee, fill_ts, raw) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
            .bind(&f.order_id)
            .bind(&f.symbol)
            .bind(&f.symbol) // placeholder for side
            .bind(f.qty.abs())
            .bind(f.price)
            .bind(fee)
            .bind(f.ts)
            .bind(raw)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn persist_position_snapshot(&self, symbol: &str, ts: DateTime<Utc>, pos: &PositionState) -> Result<()> {
        let meta = serde_json::json!({});
        sqlx::query(
            "INSERT INTO positions_snapshot(symbol, ts, net_qty, avg_price, realized_pnl, total_fees, meta) VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
            .bind(symbol)
            .bind(ts)
            .bind(pos.net_qty)
            .bind(if pos.net_qty.abs() > 0.0 { Some(pos.avg_price) } else { Option::<f64>::None })
            .bind(pos.realized_pnl)
            .bind(pos.total_fees)
            .bind(meta)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn match_via_tape(&self, order: &OrderIntent) -> Result<Vec<Fill>> {
        let mut remaining = order.qty;
        let mut fills: Vec<Fill> = Vec::new();
        let window_end = order.ts + chrono::Duration::seconds(FILL_WINDOW_SECS);

        let mut stream = sqlx::query("SELECT trade_time, raw FROM trades WHERE symbol = $1 AND trade_time >= $2 AND trade_time <= $3 ORDER BY trade_time ASC, trade_id ASC")
            .bind(&order.symbol)
            .bind(order.ts)
            .bind(window_end)
            .fetch(&self.pool);

        while let Some(row) = stream.try_next().await? {
            let trade_time: DateTime<Utc> = row.get("trade_time");
            let raw: serde_json::Value = row.get("raw");

            // Prefer parsing into TradeEvent, else fallback to fields
            if let Ok(tr) = serde_json::from_value::<TradeEvent>(raw.clone()) {
                let trade_qty = tr.q.parse::<f64>().unwrap_or(0.0);
                let trade_price = tr.p.parse::<f64>().unwrap_or(0.0);
                if trade_qty <= 0.0 { continue; }
                let take = remaining.min(trade_qty);
                if take <= 0.0 { continue; }
                let fill = Fill {
                    order_id: order.id.clone(),
                    symbol: order.symbol.clone(),
                    price: trade_price,
                    qty: take,
                    ts: trade_time,
                };
                fills.push(fill);
                remaining -= take;
                if remaining <= 1e-12 { break; }
            } else {
                // fallback: try minimal fields
                if let Some(obj) = raw.as_object() {
                    if let (Some(pv), Some(qv)) = (obj.get("p"), obj.get("q")) {
                        if let (Some(pstr), Some(qstr)) = (pv.as_str(), qv.as_str()) {
                            let price = pstr.parse::<f64>().unwrap_or(0.0);
                            let qty = qstr.parse::<f64>().unwrap_or(0.0);
                            if qty <= 0.0 { continue; }
                            let take = remaining.min(qty);
                            if take <= 0.0 { continue; }
                            // trade timestamp fallback
                            let trade_time = obj.get("T").and_then(|v| v.as_i64()).map(|ms| {
                                let naive = NaiveDateTime::from_timestamp(ms / 1000, ((ms % 1000) as u32) * 1_000_000);
                                DateTime::<Utc>::from_utc(naive, Utc)
                            }).unwrap_or_else(|| Utc::now());
                            let fill = Fill {
                                order_id: order.id.clone(),
                                symbol: order.symbol.clone(),
                                price,
                                qty: take,
                                ts: trade_time,
                            };
                            fills.push(fill);
                            remaining -= take;
                            if remaining <= 1e-12 { break; }
                        }
                    }
                }
            }
        }

        Ok(fills)
    }

    fn apply_fill_to_position(pos: &mut PositionState, side: &str, qty: f64, price: f64) -> (f64, f64) {
        let fee = price * qty * FEE_RATE;
        match side {
            "buy" => {
                if pos.net_qty >= 0.0 {
                    let total_cost = pos.avg_price * pos.net_qty + price * qty;
                    let new_qty = pos.net_qty + qty;
                    pos.avg_price = if new_qty.abs() > 0.0 { total_cost / new_qty } else { 0.0 };
                    pos.net_qty = new_qty;
                    pos.total_fees += fee;
                    (0.0, fee)
                } else {
                    let closing = qty.min(pos.net_qty.abs());
                    let realized = (pos.avg_price - price) * closing;
                    pos.net_qty += qty;
                    if pos.net_qty > 0.0 { pos.avg_price = price; }
                    pos.realized_pnl += realized;
                    pos.total_fees += fee;
                    (realized, fee)
                }
            }
            "sell" => {
                if pos.net_qty <= 0.0 {
                    let total_cost = pos.avg_price * pos.net_qty.abs() + price * qty;
                    let new_qty = pos.net_qty - qty;
                    pos.avg_price = if new_qty.abs() > 0.0 { total_cost / new_qty.abs() } else { 0.0 };
                    pos.net_qty = new_qty;
                    pos.total_fees += fee;
                    (0.0, fee)
                } else {
                    let closing = qty.min(pos.net_qty);
                    let realized = (price - pos.avg_price) * closing;
                    pos.net_qty -= qty;
                    if pos.net_qty < 0.0 { pos.avg_price = price; }
                    pos.realized_pnl += realized;
                    pos.total_fees += fee;
                    (realized, fee)
                }
            }
            _ => (0.0, fee),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                ord = self.order_rx.recv() => {
                    match ord {
                        Some(o) => {
                            // persist order record
                            if let Err(e) = self.persist_order(&o).await {
                                eprintln!("persist order err: {}", e);
                            }

                            // find fills from tape
                            let fills = match self.match_via_tape(&o).await {
                                Ok(f) => f,
                                Err(e) => {
                                    eprintln!("match error: {}", e);
                                    Vec::new()
                                }
                            };

                            for f in fills {
                                // publish fill in-process
                                let _ = self.fill_tx.send(f.clone());

                                // persist fill to DB
                                if let Err(e) = self.persist_fill_db(&f).await {
                                    eprintln!("persist_fill_db err: {}", e);
                                }

                                // update positions: lock briefly, mutate, clone snapshot, drop lock
                                let pos_snapshot_opt = {
                                    let mut positions = self.positions.lock().unwrap();
                                    let pos = positions.entry(f.symbol.clone()).or_insert_with(PositionState::new);
                                    let side = &o.side;
                                    let (_realized, _fee) = Self::apply_fill_to_position(pos, side.as_str(), f.qty, f.price);
                                    Some(pos.clone())
                                }; // lock released here

                                // persist position snapshot (await allowed — lock not held)
                                if let Some(pos_snap) = pos_snapshot_opt {
                                    if let Err(e) = self.persist_position_snapshot(&f.symbol, f.ts, &pos_snap).await {
                                        eprintln!("persist_position_snapshot err: {}", e);
                                    }
                                }

                                // update order status based on fills in DB
                                match Self::is_order_filled(&self.pool, &o.id).await {
                                    Ok(true) => {
                                        if let Err(e) = sqlx::query("UPDATE orders SET status = $1 WHERE id = $2").bind("filled").bind(&o.id).execute(&self.pool).await {
                                            eprintln!("update order status err: {}", e);
                                        }
                                    }
                                    Ok(false) => {
                                        if let Err(e) = sqlx::query("UPDATE orders SET status = $1 WHERE id = $2").bind("partially_filled").bind(&o.id).execute(&self.pool).await {
                                            eprintln!("update order status err: {}", e);
                                        }
                                    }
                                    Err(e) => eprintln!("is_order_filled err: {}", e),
                                }
                            }
                        }
                        None => break,
                    }
                }

                ev = self.depth_rx.recv() => {
                    match ev {
                        Ok(MarketEvent::Depth(_)) => { /* ignored for tape-only matching */ }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            }
        }

        Ok(())
    }

    async fn is_order_filled(pool: &PgPool, order_id: &str) -> Result<bool> {
        // sum fills
        let total: f64 = sqlx::query_scalar("SELECT COALESCE(SUM(qty),0.0) FROM fills WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(pool)
            .await?;

        // original order qty
        let orig_qty: f64 = sqlx::query_scalar("SELECT qty FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(pool)
            .await
            .unwrap_or(0.0);

        Ok(total + 1e-12 >= orig_qty)
    }
}
