use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use futures_util::StreamExt;
use ordered_float::OrderedFloat;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use url::Url;

// ----- types -----

type Price = OrderedFloat<f64>;
type Qty = f64;
type SideMap = HashMap<Price, Qty>;

#[derive(Debug, Deserialize)]
struct CombinedMsg {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TradeEvent {
    e: String,
    E: i64,
    s: String,
    t: u64,
    p: String,
    q: String,
    T: i64,
    m: bool,
    M: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct DepthUpdate {
    e: String,
    E: i64,
    s: String,
    U: u64,
    u: u64,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    lastUpdateId: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Clone)]
struct InMemoryBook {
    bids: SideMap,
    asks: SideMap,
    update_id: u64,
}

impl InMemoryBook {
    fn new() -> Self {
        Self {
            bids: HashMap::new(),
            asks: HashMap::new(),
            update_id: 0,
        }
    }
    fn apply_side(side: &mut SideMap, price: Price, qty: Qty) {
        if qty <= 0.0 {
            side.remove(&price);
        } else {
            side.insert(price, qty);
        }
    }
    fn apply_update(&mut self, upd: &DepthUpdate) {
        for lvl in &upd.b {
            let p = OrderedFloat(lvl[0].parse().unwrap_or(0.0));
            let q = lvl[1].parse().unwrap_or(0.0);
            Self::apply_side(&mut self.bids, p, q);
        }
        for lvl in &upd.a {
            let p = OrderedFloat(lvl[0].parse().unwrap_or(0.0));
            let q = lvl[1].parse().unwrap_or(0.0);
            Self::apply_side(&mut self.asks, p, q);
        }
        self.update_id = upd.u;
    }
    fn load_snapshot(&mut self, snap: Snapshot) {
        self.bids.clear();
        self.asks.clear();
        for lvl in snap.bids {
            let p = OrderedFloat(lvl[0].parse().unwrap_or(0.0));
            let q = lvl[1].parse().unwrap_or(0.0);
            if q > 0.0 {
                self.bids.insert(p, q);
            }
        }
        for lvl in snap.asks {
            let p = OrderedFloat(lvl[0].parse().unwrap_or(0.0));
            let q = lvl[1].parse().unwrap_or(0.0);
            if q > 0.0 {
                self.asks.insert(p, q);
            }
        }
        self.update_id = snap.lastUpdateId;
    }
}

// ----- bar state -----

#[derive(Clone)]
struct BarState {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vol: f64,
    start: DateTime<Utc>,
}

impl BarState {
    fn new(start: DateTime<Utc>) -> Self {
        Self {
            open: 0.0,
            high: f64::MIN,
            low: f64::MAX,
            close: 0.0,
            vol: 0.0,
            start,
        }
    }
    fn reset(&mut self, start: DateTime<Utc>) {
        self.open = 0.0;
        self.high = f64::MIN;
        self.low = f64::MAX;
        self.close = 0.0;
        self.vol = 0.0;
        self.start = start;
    }
    fn observe_trade(&mut self, price: f64, qty: f64) {
        if self.open == 0.0 {
            self.open = price;
        }
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.vol += qty;
    }
    fn is_active(&self) -> bool {
        self.open != 0.0
    }
}

// ----- helpers: DB writes -----

async fn write_trade(pool: &PgPool, sym: &str, te: &TradeEvent) -> Result<()> {
    let price = te.p.parse::<f64>().unwrap_or(0.0);
    let qty = te.q.parse::<f64>().unwrap_or(0.0);
    let trade_ts = NaiveDateTime::from_timestamp_opt(te.T / 1000, ((te.T % 1000) as u32) * 1_000_000)
        .map(|n| DateTime::<Utc>::from_naive_utc_and_offset(n, Utc));
    let event_ts = NaiveDateTime::from_timestamp_opt(te.E / 1000, ((te.E % 1000) as u32) * 1_000_000)
        .map(|n| DateTime::<Utc>::from_naive_utc_and_offset(n, Utc))
        .unwrap_or_else(Utc::now);

    let raw = serde_json::to_value(te)?;
    sqlx::query(
        "INSERT INTO trades(symbol, trade_id, event_time, trade_time, price, qty, is_buyer_maker, raw)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
    )
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

async fn write_top15_book(pool: &PgPool, symbol: &str, book: &InMemoryBook) -> Result<()> {
    let mut bids: Vec<_> = book
        .bids
        .iter()
        .map(|(p, q)| (p.into_inner(), *q))
        .collect();
    let mut asks: Vec<_> = book
        .asks
        .iter()
        .map(|(p, q)| (p.into_inner(), *q))
        .collect();
    bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    bids.truncate(15);
    asks.truncate(15);
    let raw = serde_json::json!({ "bids": bids, "asks": asks });
    sqlx::query("INSERT INTO book_events(symbol, ts, book_json) VALUES ($1, now(), $2)")
        .bind(symbol)
        .bind(raw)
        .execute(pool)
        .await?;
    Ok(())
}

async fn write_bar(
    pool: &PgPool,
    symbol: &str,
    ts: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vol: f64,
) -> Result<()> {
    sqlx::query("INSERT INTO bars_15m(symbol, ts, open, high, low, close, volume)
                 VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .bind(symbol)
        .bind(ts)
        .bind(open)
        .bind(high)
        .bind(low)
        .bind(close)
        .bind(vol)
        .execute(pool)
        .await?;
    Ok(())
}

// ----- snapshot alignment -----

async fn fetch_snapshot(client: &Client, symbol: &str) -> Result<Snapshot> {
    let url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=5000", symbol);
    Ok(client.get(&url).send().await?.json().await?)
}

async fn align_snapshot(
    client: &Client,
    buffer: &Arc<Mutex<VecDeque<DepthUpdate>>>,
    book: &Arc<Mutex<InMemoryBook>>,
    symbol: &str,
) -> Result<()> {
    const RETRY_MS: u64 = 200;
    loop {
        let snap = fetch_snapshot(client, symbol).await?;
        let first_ev = { buffer.lock().unwrap().front().cloned() };
        if let Some(ev) = first_ev {
            if snap.lastUpdateId < ev.U {
                tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
                continue;
            }
            {
                let mut q = buffer.lock().unwrap();
                while q.front().map_or(false, |e| e.u <= snap.lastUpdateId) {
                    q.pop_front();
                }
            }
            if let Some(next_ev) = { buffer.lock().unwrap().front().cloned() } {
                if !(next_ev.U <= snap.lastUpdateId + 1 && next_ev.u >= snap.lastUpdateId + 1) {
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
                    continue;
                }
            }
            book.lock().unwrap().load_snapshot(snap);
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(RETRY_MS)).await;
    }
}

// ----- tasks extracted -----

fn spawn_reader_task(
    mut read: impl futures_util::stream::Stream<
            Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
        > + Unpin
        + Send
        + 'static,
    buffer: Arc<Mutex<VecDeque<DepthUpdate>>>,
    pool: PgPool,
    bar_state: Arc<Mutex<BarState>>,
    symbol: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(msg_res) = read.next().await {
            match msg_res {
                Ok(msg) => {
                    if !msg.is_text() {
                        continue;
                    }
                    let txt = match msg.into_text() {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    if let Ok(c) = serde_json::from_str::<CombinedMsg>(&txt) {
                        let data = c.data.to_string();
                        if let Ok(upd) = serde_json::from_str::<DepthUpdate>(&data) {
                            buffer.lock().unwrap().push_back(upd);
                            continue;
                        }
                        if let Ok(tr) = serde_json::from_str::<TradeEvent>(&data) {
                            let pool_c = pool.clone();
                            let sym = symbol.clone();
                            // write trade and update bar state
                            let price = tr.p.parse::<f64>().unwrap_or(0.0);
                            let qty = tr.q.parse::<f64>().unwrap_or(0.0);
                            tokio::spawn(async move {
                                let _ = write_trade(&pool_c, &sym, &tr).await;
                            });
                            let mut bs = bar_state.lock().unwrap();
                            bs.observe_trade(price, qty);
                            continue;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    })
}

async fn process_pending_updates(
    buffer: &Arc<Mutex<VecDeque<DepthUpdate>>>,
    book: &Arc<Mutex<InMemoryBook>>,
    pool: &PgPool,
    symbol: &str,
) -> Result<()> {
    let mut pending = Vec::new();
    {
        let mut q = buffer.lock().unwrap();
        while let Some(ev) = q.pop_front() {
            pending.push(ev);
        }
    }
    for ev in pending {
        book.lock().unwrap().apply_update(&ev);
        write_top15_book(pool, symbol, &book.lock().unwrap()).await?;
    }
    Ok(())
}

async fn maybe_close_bar(bar_state: &Arc<Mutex<BarState>>, pool: &PgPool, symbol: &str) -> Result<()> {
    let mut bs = bar_state.lock().unwrap();
    if bs.is_active() && (Utc::now() - bs.start) >= Duration::minutes(15) {
        // persist on close
        write_bar(pool, symbol, bs.start, bs.open, bs.high, bs.low, bs.close, bs.vol).await?;
        bs.reset(Utc::now());
    }
    Ok(())
}

// ----- main -----

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let symbol = "BTCUSDT".to_string();
    let streams = format!("{}@depth@100ms/{}@trade", symbol.to_lowercase(), symbol.to_lowercase());
    let ws_url = format!("wss://stream.binance.com:9443/stream?streams={}", streams);

    let client = Client::new();
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let url = Url::parse(&ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (_write, read) = ws_stream.split();

    let buffer = Arc::new(Mutex::new(VecDeque::new()));
    let book = Arc::new(Mutex::new(InMemoryBook::new()));
    let bar_state = Arc::new(Mutex::new(BarState::new(Utc::now())));

    let reader = spawn_reader_task(read, buffer.clone(), pool.clone(), bar_state.clone(), symbol.clone());

    align_snapshot(&client, &buffer, &book, &symbol).await?;

    // main processing loop: handle pending updates, maybe close bar.
    loop {
        process_pending_updates(&buffer, &book, &pool, &symbol).await?;
        maybe_close_bar(&bar_state, &pool, &symbol).await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // reader.await.ok(); // unreachable
}
