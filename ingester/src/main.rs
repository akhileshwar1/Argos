use anyhow::Result;
use reqwest::Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use url::Url;
use futures_util::{StreamExt};
use std::sync::{Arc, Mutex};
use ordered_float::OrderedFloat;

#[derive(Debug, Deserialize)]
struct CombinedMsg {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TradeEvent {
    e: String,      // "trade"
    E: i64,         // event time (ms)
    s: String,      // symbol
    t: u64,         // trade id
    p: String,      // price as string
    q: String,      // qty as string
    T: i64,         // trade time (ms)
    m: bool,        // is buyer maker
    M: bool,
}

async fn write_trade(pool: &PgPool, sym: &str, te: &TradeEvent) -> Result<()> {
    let price = te.p.parse::<f64>().unwrap_or(0.0);
    let qty   = te.q.parse::<f64>().unwrap_or(0.0);
    let trade_ts = chrono::NaiveDateTime::from_timestamp_opt(te.T / 1000, ((te.T % 1000) as u32) * 1_000_000)
        .map(|n| chrono::DateTime::<Utc>::from_utc(n, Utc));
    let event_ts = chrono::NaiveDateTime::from_timestamp_opt(te.E / 1000, ((te.E % 1000) as u32) * 1_000_000)
        .map(|n| chrono::DateTime::<Utc>::from_utc(n, Utc)).unwrap_or_else(|| chrono::Utc::now());

    let raw = serde_json::to_value(te)?;
    sqlx::query("INSERT INTO trades(symbol, trade_id, event_time, trade_time, price, qty, is_buyer_maker, raw) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(sym)
        .bind(te.t as i64)
        .bind(event_ts)
        .bind(trade_ts)
        .bind(price)
        .bind(qty)
        .bind(te.m)
        .bind(raw)
        .execute(pool)
        .await?;
    Ok(())
}

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

async fn write_snapshot(pool: &PgPool, symbol: &str, seq_id: u64, bids: &SideMap, asks: &SideMap) -> Result<()> {
    // convert SideMap -> Vec<[f64,f64]> for JSON
    let bids_vec: Vec<(f64,f64)> = bids.iter().map(|(p,q)| (p.into_inner(), *q)).collect();
    let asks_vec: Vec<(f64,f64)> = asks.iter().map(|(p,q)| (p.into_inner(), *q)).collect();
    let raw = serde_json::json!({ "bids": bids_vec, "asks": asks_vec, "seq_id": seq_id });

    sqlx::query("INSERT INTO depth_snapshots(symbol, snapshot_time, seq_id, bids, asks, raw) VALUES ($1, now(), $2, $3, $4, $5)")
        .bind(symbol)
        .bind(seq_id as i64)
        .bind(serde_json::to_value(&bids_vec)?)
        .bind(serde_json::to_value(&asks_vec)?)
        .bind(raw)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Debug, Serialize, Clone)]
struct BookEvent {
    symbol: String,
    ts: chrono::DateTime<Utc>,
    best_bid_price: Option<f64>,
    best_bid_qty: Option<f64>,
    best_ask_price: Option<f64>,
    best_ask_qty: Option<f64>,
    imbalance: Option<f64>,
    book_json: serde_json::Value,
}

async fn write_and_broadcast_book(
    pool: &PgPool,
    book_evt_tx: &broadcast::Sender<BookEvent>,
    symbol: &str,
    bids: &SideMap,
    asks: &SideMap,
) -> Result<()> {
    // get best bid/ask (OrderedFloat -> f64)
    let best_bid = bids.iter().max_by_key(|(p, _q)| *p).map(|(p,q)| (p.into_inner(), *q));
    let best_ask = asks.iter().min_by_key(|(p, _q)| *p).map(|(p,q)| (p.into_inner(), *q));

    let imbalance = match (best_bid, best_ask) {
        (Some((bp, bq)), Some((ap, aq))) => Some((bq - aq) / (bq + aq + 1e-12)),
        _ => None,
    };

    // optional compact book JSON (top N if you want; here we serialize full maps)
    let bids_vec: Vec<(f64,f64)> = bids.iter().map(|(p,q)| (p.into_inner(), *q)).collect();
    let asks_vec: Vec<(f64,f64)> = asks.iter().map(|(p,q)| (p.into_inner(), *q)).collect();
    let book_json = serde_json::json!({ "bids": bids_vec, "asks": asks_vec });

    let ev = BookEvent {
        symbol: symbol.to_string(),
        ts: chrono::Utc::now(),
        best_bid_price: best_bid.map(|(p, _)| p),
        best_bid_qty: best_bid.map(|(_, q)| q),
        best_ask_price: best_ask.map(|(p, _)| p),
        best_ask_qty: best_ask.map(|(_, q)| q),
        imbalance,
        book_json: book_json.clone(),
    };

    // write to DB
    sqlx::query("INSERT INTO book_events(symbol, ts, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, imbalance, book_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
        .bind(symbol)
        .bind(ev.ts)
        .bind(ev.best_bid_price)
        .bind(ev.best_bid_qty)
        .bind(ev.best_ask_price)
        .bind(ev.best_ask_qty)
        .bind(ev.imbalance)
        .bind(book_json)
        .execute(pool)
        .await?;

    // broadcast in-process for UI/strategy
    let _ = book_evt_tx.send(ev);

    Ok(())
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
    book_arc: &Arc<Mutex<InMemoryBook>>,
    buffer: &Arc<Mutex<VecDeque<DepthUpdate>>>,
    pool: &PgPool,
    depth_tx: &broadcast::Sender<DepthUpdate>,
    book_evt_tx: &broadcast::Sender<BookEvent>,
    symbol: &str,
) -> Result<()> {
    // drain buffer quickly
    let mut pending: Vec<DepthUpdate> = Vec::new();
    {
        let mut q = buffer.lock().unwrap();
        while let Some(ev) = q.pop_front() {
            pending.push(ev);
        }
    } // release lock

    for ev in pending {
        // update book under lock, but keep lock time minimal
        {
            let mut book = book_arc.lock().unwrap();
            if ev.u < book.update_id { continue; }
            if ev.U > book.update_id + 1 {
                return Err(anyhow::anyhow!("Gap detected; restart"));
            }
            book.apply_update(&ev);
            // copy small slices for later publishing
        } // release lock here

        // persist raw event
        write_raw_event(pool, symbol, &ev).await?;

        // broadcast raw depth update
        let _ = depth_tx.send(ev.clone());

        // build and publish book event: lock briefly to copy current book maps
        let (bids_copy, asks_copy) = {
            let book = book_arc.lock().unwrap();
            (book.bids.clone(), book.asks.clone())
        };
        // write and broadcast the compact book event (await)
        let _ = write_and_broadcast_book(pool, book_evt_tx, symbol, &bids_copy, &asks_copy).await;
    }

    Ok(())
}

fn spawn_reader_task(
    mut read: impl futures_util::stream::Stream<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>> + Unpin + Send + 'static,
    buffer: Arc<Mutex<VecDeque<DepthUpdate>>>,
    pool: PgPool,
    trade_tx: broadcast::Sender<TradeEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(msg_res) = read.next().await {
            match msg_res {
                Ok(msg) => {
                    if !msg.is_text() { continue; }
                    let txt = match msg.into_text() { Ok(t) => t, Err(_) => continue };
                    // Try combined wrapper first
                    if let Ok(c) = serde_json::from_str::<CombinedMsg>(&txt) {
                        if let Some(data_txt) = c.data.as_object().and_then(|_| Some(c.data.to_string())) {
                            // parse data_txt as depth or trade
                            if let Ok(upd) = serde_json::from_str::<DepthUpdate>(&data_txt) {
                                let mut q = buffer.lock().unwrap();
                                q.push_back(upd);
                                continue;
                            }
                            if let Ok(tr) = serde_json::from_str::<TradeEvent>(&data_txt) {
                                let pool_c = pool.clone();
                                let sym = tr.s.clone();
                                let tx_clone = trade_tx.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = write_trade(&pool_c, &sym, &tr).await {
                                        eprintln!("trade write err: {}", e);
                                    }
                                    let _ = tx_clone.send(tr);
                                });
                                continue;
                            }
                        }
                        continue;
                    }
                }
                Err(_) => break,
            }
        }
    })
}

fn spawn_snapshot_task(
    book_arc: Arc<Mutex<InMemoryBook>>,
    pool: PgPool,
    symbol: String,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            let (seq_id, bids_copy, asks_copy) = {
                let b = book_arc.lock().unwrap();
                (b.update_id, b.bids.clone(), b.asks.clone())
            };
            if let Err(e) = write_snapshot(&pool, &symbol, seq_id, &bids_copy, &asks_copy).await {
                eprintln!("snapshot write error: {}", e);
            }
        }
    })
}

async fn align_snapshot(
    client: &reqwest::Client,
    buffer: &Arc<Mutex<VecDeque<DepthUpdate>>>,
    book: &Arc<Mutex<InMemoryBook>>,
    symbol: &str,
) -> anyhow::Result<()> {
    const RETRY_MS: u64 = 200;
    loop {
        let snap = fetch_snapshot(client, symbol).await?;
        // peek first buffered event (clone while holding lock briefly)
        let first_opt = {
            let q = buffer.lock().unwrap();
            q.front().cloned()
        };

        if let Some(first_ev) = first_opt {
            // snapshot too old: fetch again
            if snap.lastUpdateId < first_ev.U {
                tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
                continue;
            }

            // drop any buffered event where u <= lastUpdateId
            {
                let mut q = buffer.lock().unwrap();
                while q.front().map_or(false, |e| e.u <= snap.lastUpdateId) {
                    q.pop_front();
                }
            }

            // peek again after trimming
            let first_after = {
                let q = buffer.lock().unwrap();
                q.front().cloned()
            };

            if let Some(first_ev) = first_after {
                // check alignment condition per Binance doc
                if !(first_ev.U <= snap.lastUpdateId + 1 && first_ev.u >= snap.lastUpdateId + 1) {
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
                    continue;
                }
            } else {
                // buffer empty (wait for more WS messages)
                tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
                continue;
            }

            // success: load snapshot under lock and return
            {
                let mut b = book.lock().unwrap();
                b.load_snapshot(snap);
            }
            return Ok(());
        } else {
            // buffer empty -> wait for websocket to fill it
            tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok(); // load .env into process env
    // config
    let symbol = "BTCUSDT".to_string(); // uppercase
    
    // combined stream (depth + trade)
    let streams = format!("{}@depth@100ms/{}@trade", symbol.to_lowercase(), symbol.to_lowercase());
    let ws_url = format!("wss://stream.binance.com:9443/stream?streams={}", streams);

    let client = Client::new();
    let pool = PgPool::connect(std::env::var("DATABASE_URL")?.as_str()).await?;
    let (tx, _rx) = broadcast::channel::<DepthUpdate>(1024);

    // connect websocket
    let url = Url::parse(&ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (_write, read) = ws_stream.split(); // we don't send, so ignore write

    // buffer incoming raw messages until snapshot is applied
    let buffer: Arc<Mutex<VecDeque<DepthUpdate>>> = Arc::new(Mutex::new(VecDeque::new()));
    let book = Arc::new(Mutex::new(InMemoryBook::new()));

    // new broadcast channel for compact book events
    let (book_evt_tx, _book_evt_rx) = broadcast::channel::<BookEvent>(1024);

    // new broadcast channel for compact trade events
    let (trade_tx, _trade_rx) = broadcast::channel::<TradeEvent>(1024);

    let reader_handle = spawn_reader_task(read, buffer.clone(), pool.clone(), trade_tx.clone());

    let book_for_snap = book.clone();
    let pool_for_snap = pool.clone();
    let symbol_for_snap = symbol.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let (seq_id, bids_copy, asks_copy) = {
                let b = book_for_snap.lock().unwrap();
                (b.update_id, b.bids.clone(), b.asks.clone())
            };
            if let Err(e) = write_snapshot(&pool_for_snap, &symbol_for_snap, seq_id, &bids_copy, &asks_copy).await {
                eprintln!("snapshot write error: {}", e);
            }
        }
    });

    align_snapshot(&client, &buffer, &book, &symbol).await?;

    // apply buffered events then continue processing live
    apply_updates(&book, &buffer, &pool, &tx, &book_evt_tx, &symbol).await?;

    // main loop: regularly drain buffer and process updates
    loop {
        apply_updates(&book, &buffer, &pool, &tx, &book_evt_tx, &symbol).await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
