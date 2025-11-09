// src/tui.rs
use crate::core::*;
use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::{sleep, Duration};

/// Extremely light "TUI": periodically prints the latest bar, recent trades, orders and fills.
/// In future you can replace this with a proper curses/ratatui UI.
pub async fn run_tui(
    mut evt_rx: Receiver<MarketEvent>,
    mut fill_rx: Receiver<Fill>,
) {
    // small in-memory caches
    let bars: Arc<Mutex<Vec<Bar>>> = Arc::new(Mutex::new(Vec::new()));
    let trades: Arc<Mutex<Vec<TradeEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let fills: Arc<Mutex<Vec<Fill>>> = Arc::new(Mutex::new(Vec::new()));

    // spawn a task to collect events into caches
    {
        let bars = bars.clone();
        let trades = trades.clone();
        tokio::spawn(async move {
            while let Ok(ev) = evt_rx.recv().await {
                match ev {
                    MarketEvent::Bar(b) => {
                        let mut bb = bars.lock().unwrap();
                        bb.push(b);
                        if bb.len() > 50 { bb.remove(0); }
                    }
                    MarketEvent::Trade(t) => {
                        let mut tt = trades.lock().unwrap();
                        tt.push(t);
                        if tt.len() > 100 { tt.remove(0); }
                    }
                    _ => {}
                }
            }
        });
    }

    // collect fills
    {
        let fills = fills.clone();
        tokio::spawn(async move {
            while let Ok(f) = fill_rx.recv().await {
                let mut ff = fills.lock().unwrap();
                ff.push(f);
                if ff.len() > 100 { ff.remove(0); }
            }
        });
    }

    // simple periodic render
    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        // clear screen (simple)
        print!("\x1B[2J\x1B[1;1H");
        println!("SIMULATOR TUI  — live view @ {}", Utc::now());
        println!("--------------------------------------------------");

        // show last bar
        if let Some(last) = bars.lock().unwrap().last().cloned() {
            println!("Last Bar: {} {} O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.6}", last.symbol, last.ts, last.o, last.h, last.l, last.c, last.v);
        } else {
            println!("No bars yet.");
        }

        // top of trade tape
        let tlist = trades.lock().unwrap();
        println!("-- Recent trades (latest first) --");
        for t in tlist.iter().rev().take(10) {
            println!(" {} | p={} q={} ms={} m={}", t.s, t.p, t.q, t.E, t.m);
        }

        // fills
        println!("-- Recent fills (latest first) --");
        let fl = fills.lock().unwrap();
        for f in fl.iter().rev().take(10) {
            println!(" {} {}@{:.2} qty={:.6} at {}", f.order_id, f.symbol, f.price, f.qty, f.ts);
        }

        println!("--------------------------------------------------");
        println!("(ctrl-c to quit)");
    }
}
