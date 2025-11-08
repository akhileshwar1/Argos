use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use url::Url;
use futures_util::{StreamExt};
use std::sync::{Arc, Mutex};
use ordered_float::OrderedFloat;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct DepthUpdate {
    e: String,
    E: i64,
    s: String,
    U: u64,
    u: u64,
    b: Vec<[String;2]>,
    a: Vec<[String;2]>,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    lastUpdateId: u64,
    bids: Vec<[String;2]>,
    asks: Vec<[String;2]>,
}

type Price = OrderedFloat<f64>;
type Qty = f64;
type SideMap = HashMap<Price, Qty>;

#[derive(Clone)]
pub struct InMemoryBook {
    pub bids: SideMap,
    pub asks: SideMap,
    pub update_id: u64,
}

impl InMemoryBook {
    pub fn new() -> Self {
        Self { bids: HashMap::new(), asks: HashMap::new(), update_id: 0 }
    }

    fn apply_side(side: &mut SideMap, price: Price, qty: Qty) {
        if qty <= 0.0 { side.remove(&price); } else { side.insert(price, qty); }
    }

    pub fn apply_update(&mut self, upd: &DepthUpdate) {
        for lvl in upd.b.iter() {
            let p: Price = OrderedFloat(lvl[0].parse::<f64>().unwrap_or(0.0));
            let q: Qty   = lvl[1].parse().unwrap_or(0.0);
            Self::apply_side(&mut self.bids, p, q);
        }
        for lvl in upd.a.iter() {
            let p: Price = OrderedFloat(lvl[0].parse::<f64>().unwrap_or(0.0));
            let q: Qty   = lvl[1].parse().unwrap_or(0.0);
            Self::apply_side(&mut self.asks, p, q);
        }
        self.update_id = upd.u;
    }

    pub fn load_snapshot(&mut self, snap: Snapshot) {
        self.bids.clear(); self.asks.clear();
        for lvl in snap.bids {
            let p: Price = OrderedFloat(lvl[0].parse::<f64>().unwrap_or(0.0));
            let q: Qty = lvl[1].parse().unwrap_or(0.0);
            if q > 0.0 { self.bids.insert(p,q); }
        }
        for lvl in snap.asks {
            let p: Price = OrderedFloat(lvl[0].parse::<f64>().unwrap_or(0.0));
            let q: Qty = lvl[1].parse().unwrap_or(0.0);
            if q > 0.0 { self.asks.insert(p,q); }
        }
        self.update_id = snap.lastUpdateId;
    }
}

async fn fetch_snapshot(client: &Client, symbol: &str) -> Result<Snapshot> {
    let url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=5000", symbol);
    let snap: Snapshot = client.get(&url).send().await?.json().await?;
    Ok(snap)
}

async fn write_raw_event(pool: &PgPool, sym: &str, ev: &DepthUpdate) -> Result<()> {
    // Insert raw JSONB via bind (use sqlx feature "json" / "postgres" enabled)
    let raw_val = serde_json::to_value(ev)?;
    sqlx::query("INSERT INTO depth_raw(symbol, event_time, raw) VALUES ($1, to_timestamp($2::double precision / 1000.0), $3)")
        .bind(sym)
        .bind(ev.E as f64)
        .bind(raw_val)
        .execute(pool).await?;
    Ok(())
}

/// Drain the shared buffer into a local vec and process each event.
/// This avoids holding the mutex while doing async DB writes.
async fn apply_updates(
    book: &mut InMemoryBook,
    buffer: &Arc<Mutex<VecDeque<DepthUpdate>>>,
    pool: &PgPool,
    tx: &broadcast::Sender<DepthUpdate>,
    symbol: &str,
) -> Result<()> {
    // take-out pending events quickly
    let mut pending: Vec<DepthUpdate> = Vec::new();
    {
        let mut q = buffer.lock().unwrap();
        while let Some(ev) = q.pop_front() {
            pending.push(ev);
        }
    } // lock released

    for ev in pending {
        if ev.u < book.update_id { continue; }
        if ev.U > book.update_id + 1 {
            return Err(anyhow::anyhow!("Gap detected; restart"));
        }
        book.apply_update(&ev);
        write_raw_event(pool, symbol, &ev).await?;
        let _ = tx.send(ev.clone()); // ignore error (no receivers)
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok(); // load .env into process env
    // config
    let symbol = "BTCUSDT".to_string(); // uppercase
    let ws_stream_name = format!("{}@depth@100ms", symbol.to_lowercase());
    let ws_url = format!("wss://stream.binance.com:9443/ws/{}", ws_stream_name);
    let client = Client::new();
    let pool = PgPool::connect(std::env::var("DATABASE_URL")?.as_str()).await?;
    let (tx, _rx) = broadcast::channel::<DepthUpdate>(1024);

    // connect websocket
    let url = Url::parse(&ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (_write, read) = ws_stream.split(); // we don't send, so ignore write

    // buffer incoming raw messages until snapshot is applied
    let buffer: Arc<Mutex<VecDeque<DepthUpdate>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut book = InMemoryBook::new();

    // spawn a task to read and buffer events
    {
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(m) => {
                        if m.is_text() {
                            if let Ok(txt) = m.into_text() {
                                if let Ok(ev) = serde_json::from_str::<DepthUpdate>(&txt) {
                                    let mut q = buf_clone.lock().unwrap();
                                    q.push_back(ev);
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }

    // fetch snapshot and align
    loop {
        let snap = fetch_snapshot(&client, &symbol).await?;
        // peek first buffered event (clone it out while holding lock briefly)
        let first_opt = {
            let q = buffer.lock().unwrap();
            q.front().cloned()
        };
        if let Some(first_ev) = first_opt {
            if snap.lastUpdateId < first_ev.U {
                // snapshot too old: fetch again
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
            // drop any buffered event where u <= lastUpdateId
            {
                let mut q = buffer.lock().unwrap();
                while q.front().map_or(false, |e| e.u <= snap.lastUpdateId) {
                    q.pop_front();
                }
            }

            // peek again
            let first_after = {
                let q = buffer.lock().unwrap();
                q.front().cloned()
            };

            if let Some(first_ev) = first_after {
                if !(first_ev.U <= snap.lastUpdateId + 1 && first_ev.u >= snap.lastUpdateId + 1) {
                    // not aligned; refetch snapshot
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
            } else {
                // no buffered events yet; wait briefly for websocket to fill buffer
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }

            // success: load snapshot to book
            book.load_snapshot(snap);
            break;
        } else {
            // buffer empty: wait for ws messages
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    // apply buffered events then continue processing live
    apply_updates(&mut book, &buffer, &pool, &tx, &symbol).await?;

    // main loop: regularly drain buffer and process updates
    loop {
        apply_updates(&mut book, &buffer, &pool, &tx, &symbol).await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
