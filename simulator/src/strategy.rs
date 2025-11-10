// src/strategy.rs
use crate::core::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use tokio::sync::{broadcast, mpsc, broadcast::Receiver};
use uuid::Uuid;

/// Minimal mean-reversion strategy that consumes 15m Bar events and
/// emits OrderIntent (entries & exits). Designed for offline/backtest runs.
pub struct Strategy {
    pub evt_rx: Receiver<MarketEvent>,            // receives MarketEvent::Bar
    pub order_tx: mpsc::Sender<OrderIntent>,      // send intents to OMS
    pub order_pub: broadcast::Sender<OrderIntent>,// broadcast intents for TUI
    pub fill_rx: broadcast::Receiver<Fill>,       // receive fills from OMS (shadow)
}

struct OpenTrade {
    id: String,
    entry_price: f64,
    side: Side,
    qty: f64,
    bars_held: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side { Long, Short }

impl Side {
    fn opposite(&self) -> Self { match self { Side::Long => Side::Short, Side::Short => Side::Long } }
    fn as_str(&self) -> &'static str { match self { Side::Long => "buy", Side::Short => "sell" } }
}

pub struct Config {
    pub vol_mult: f64,
    pub tp_pct: f64,
    pub sl_pct: f64,
    pub max_hold_bars: usize,
    pub trade_notional: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self { vol_mult: 1.2, tp_pct: 0.8, sl_pct: 0.5, max_hold_bars: 8, trade_notional: 20.0 }
    }
}

pub struct State {
    // 15m indicators
    ema20_15m: Option<f64>,
    ema50_15m: Option<f64>,
    vol20: VecDeque<f64>, // last 20 volumes
    last_bar: Option<Bar_15m>,

    // 1h accumulators -> hourly VWAP + 1h EMAs
    current_hour_start: Option<DateTime<Utc>>,
    hour_pv_acc: f64,
    hour_v_acc: f64,
    bars_in_hour: usize,
    ema20_1h: Option<f64>,
    ema50_1h: Option<f64>,
    vwap_1h: Option<f64>,

    // trading
    open_trades: Vec<OpenTrade>,
    pending_orders: HashMap<String, OrderIntent>, // sent but not yet filled

    // config / bookkeeping
    cfg: Config,
}

impl State {
    fn new(cfg: Config) -> Self {
        Self {
            ema20_15m: None,
            ema50_15m: None,
            vol20: VecDeque::with_capacity(20),
            last_bar: None,
            current_hour_start: None,
            hour_pv_acc: 0.0,
            hour_v_acc: 0.0,
            bars_in_hour: 0,
            ema20_1h: None,
            ema50_1h: None,
            vwap_1h: None,
            open_trades: Vec::new(),
            pending_orders: HashMap::new(),
            cfg,
        }
    }

    fn alpha(period: usize) -> f64 { 2.0 / (period as f64 + 1.0) }

    fn update_ema(prev: Option<f64>, val: f64, period: usize) -> f64 {
        match prev { None => val, Some(p) => (val * Self::alpha(period)) + (p * (1.0 - Self::alpha(period))) }
    }

    fn push_vol(&mut self, v: f64) {
        if self.vol20.len() == 20 { self.vol20.pop_front(); }
        self.vol20.push_back(v);
    }

    fn vol20_avg(&self) -> f64 {
        if self.vol20.is_empty() { 0.0 } else { self.vol20.iter().sum::<f64>() / (self.vol20.len() as f64) }
    }
}

/// Note: relies on the shadow OMS to send Fill records back with `order_id` matching OrderIntent.id.
/// Fill structure must contain at least: order_id (String), filled_price (f64), filled_qty (f64), side (String).
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
        let mut state = State::new(Config::default());

        loop {
            tokio::select! {
                ev = self.evt_rx.recv() => {
                    match ev {
                        Ok(MarketEvent::Bar_15m(bar)) => {
                            self.on_bar(bar, &mut state).await?;
                        }
                        Ok(_) => { /* ignore other events in offline backtest */ }
                        Err(_) => return Ok(()), // channel closed
                    }
                }

                fill = self.fill_rx.recv() => {
                    match fill {
                        Ok(f) => {
                            self.on_fill(f, &mut state).await?;
                        }
                        Err(_) => { /* ignore */ }
                    }
                }
            }
        }
    }

    async fn on_fill(&self, f: Fill, state: &mut State) -> Result<()> {
        // try match pending order
        if let Some(intent) = state.pending_orders.remove(&f.order_id) {
            // create open trade if this was entry; if it was exit, remove matching trade(s)
            let side = if intent.side.eq_ignore_ascii_case("buy") { Side::Long } else { Side::Short };
            // If this fill is an entry (we decide entry vs exit by checking existing open trades at same symbol+side)
            let is_entry = match side {
                Side::Long => state.open_trades.iter().all(|t| t.side != Side::Long),
                Side::Short => state.open_trades.iter().all(|t| t.side != Side::Short),
            };
            if is_entry {
                // create OpenTrade
                let ot = OpenTrade { id: f.order_id.clone(), entry_price: f.price, side, qty: f.qty, bars_held: 0 };
                state.open_trades.push(ot);
            } else {
                // exit: remove trades with matching side (FIFO)
                if let Some(pos) = state.open_trades.iter().position(|t| t.side == side) {
                    state.open_trades.remove(pos);
                }
            }
        }
        Ok(())
    }

    async fn on_bar(&self, bar: Bar_15m, state: &mut State) -> Result<()> {
        // update 15m EMAs
        let close = bar.c;
        state.ema20_15m = Some(State::update_ema(state.ema20_15m, close, 20));
        state.ema50_15m = Some(State::update_ema(state.ema50_15m, close, 50));

        // vol20
        state.push_vol(bar.v);
        let vol20 = state.vol20_avg();

        // hourly VWAP / EMAs (aggregate 15m bars)
        let hour_start = chrono::DateTime::<Utc>::from_utc(
            chrono::NaiveDateTime::from_timestamp_opt(bar.ts.timestamp() - (bar.ts.timestamp() % 3600), 0).unwrap(),
            Utc,
        );

        let (new_hour_start, hpv, hv, bh, e20_1h, e50_1h, v1h) = match state.current_hour_start {
            None => (Some(hour_start), bar.c * bar.v, bar.v, 1, state.ema20_1h, state.ema50_1h, state.vwap_1h),
            Some(hs) if hs == hour_start => (state.current_hour_start, state.hour_pv_acc + (bar.c * bar.v), state.hour_v_acc + bar.v, state.bars_in_hour + 1, state.ema20_1h, state.ema50_1h, state.vwap_1h),
            Some(_) => {
                // finalize previous hour
                let prev_vwap = if state.hour_v_acc > 0.0 { Some(state.hour_pv_acc / state.hour_v_acc) } else { None };
                let e20n = prev_vwap.map(|v| State::update_ema(state.ema20_1h, v, 20)).or(state.ema20_1h);
                let e50n = prev_vwap.map(|v| State::update_ema(state.ema50_1h, v, 50)).or(state.ema50_1h);
                (Some(hour_start), bar.c * bar.v, bar.v, 1, e20n, e50n, prev_vwap)
            }
        };

        state.current_hour_start = new_hour_start;
        state.hour_pv_acc = hpv;
        state.hour_v_acc = hv;
        state.bars_in_hour = bh;
        state.ema20_1h = e20_1h;
        state.ema50_1h = e50_1h;
        state.vwap_1h = v1h;

        // check exits first (TP/SL/timeout) using existing open_trades
        let mut exits_to_send: Vec<OrderIntent> = Vec::new();
        let mut remaining_trades: Vec<OpenTrade> = Vec::new();

        for mut t in state.open_trades.drain(..) {
            t.bars_held += 1;
            let tp_price = match t.side { Side::Long => t.entry_price * (1.0 + state.cfg.tp_pct / 100.0), Side::Short => t.entry_price * (1.0 - state.cfg.tp_pct / 100.0) };
            let sl_price = match t.side { Side::Long => t.entry_price * (1.0 - state.cfg.sl_pct / 100.0), Side::Short => t.entry_price * (1.0 + state.cfg.sl_pct / 100.0) };

            let tp_hit = match t.side { Side::Long => bar.h >= tp_price, Side::Short => bar.l <= tp_price };
            let sl_hit = match t.side { Side::Long => bar.l <= sl_price, Side::Short => bar.h >= sl_price };
            let timeout = t.bars_held >= state.cfg.max_hold_bars;

            if tp_hit || sl_hit || timeout {
                // send market exit (opposite side)
                let exit_side = t.side.opposite();
                let id = Uuid::new_v4().to_string();
                let order = OrderIntent {
                    id: id.clone(),
                    symbol: bar.symbol.clone(),
                    side: exit_side.as_str().to_string(),
                    price: None,
                    qty: t.qty,
                    ts: Utc::now(),
                };
                exits_to_send.push(order);
            } else {
                remaining_trades.push(t);
            }
        }

        // publish & send exits
        for o in exits_to_send.into_iter() {
            let _ = self.order_tx.send(o.clone()).await;
            let _ = self.order_pub.send(o.clone());
        }

        // keep remaining trades
        state.open_trades = remaining_trades;

        // now entry detection (mean-reversion fade + reclaim)
        // prerequisites
        if let (Some(ema20_15m_v), Some(_ema50_15m)) = (state.ema20_15m, state.ema50_15m) {
            if let (Some(ema20_1h_v), Some(ema50_1h_v), Some(vwap_1h_v), Some(prev)) =
                (state.ema20_1h, state.ema50_1h, state.vwap_1h, state.last_bar.clone()) {

                    // 1h bias
                    let bias = if bar.c > vwap_1h_v && ema20_1h_v > ema50_1h_v { Some(Side::Long) }
                    else if bar.c < vwap_1h_v && ema20_1h_v < ema50_1h_v { Some(Side::Short) }
                    else { None };

                    // previous bar exhaustion/urgency
                    let vol_spike = if vol20 <= 0.0 { false } else { prev.v >= state.cfg.vol_mult * vol20 };
                    let range_prev = (prev.h - prev.l).max(1e-9);
                    let lower_wick_frac = (prev.c - prev.l) / range_prev;
                    let upper_wick_frac = (prev.h - prev.c) / range_prev;
                    let exhaustion_long = vol_spike && (lower_wick_frac >= 0.55);
                    let exhaustion_short = vol_spike && (upper_wick_frac >= 0.55);

                    // reclaim check (use close reclaim)
                    let eps = 0.0005 * bar.c;
                    let reclaim_long = (prev.l <= ema20_15m_v + eps) && exhaustion_long && (bar.c >= ema20_15m_v - eps);
                    let reclaim_short = (prev.h >= ema20_15m_v - eps) && exhaustion_short && (bar.c <= ema20_15m_v + eps);

                    match bias {
                        Some(Side::Long) if reclaim_long => {
                            // compute qty from trade_notional
                            let qty = (state.cfg.trade_notional / bar.c).max(0.0);
                            if qty > 0.0 {
                                let id = Uuid::new_v4().to_string();
                                let order = OrderIntent {
                                    id: id.clone(),
                                    symbol: bar.symbol.clone(),
                                    side: "buy".to_string(),
                                    price: None,
                                    qty,
                                    ts: Utc::now(),
                                };
                                state.pending_orders.insert(id.clone(), order.clone());
                                let _ = self.order_tx.send(order.clone()).await;
                                let _ = self.order_pub.send(order);
                            }
                        }
                        Some(Side::Short) if reclaim_short => {
                            let qty = (state.cfg.trade_notional / bar.c).max(0.0);
                            if qty > 0.0 {
                                let id = Uuid::new_v4().to_string();
                                let order = OrderIntent {
                                    id: id.clone(),
                                    symbol: bar.symbol.clone(),
                                    side: "sell".to_string(),
                                    price: None,
                                    qty,
                                    ts: Utc::now(),
                                };
                                state.pending_orders.insert(id.clone(), order.clone());
                                let _ = self.order_tx.send(order.clone()).await;
                                let _ = self.order_pub.send(order);
                            }
                        }
                        _ => {}
                    }
                }
        }

        // store last bar
        state.last_bar = Some(bar);

        Ok(())
    }
}
