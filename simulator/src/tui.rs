// src/tui.rs
use crate::core::*;
use anyhow::Result;
use chrono::Utc;
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph, Wrap, Table, Row, Cell},
    layout::{Constraint, Direction, Layout},
    Terminal, Frame,
};
use crossterm::{
    event::{self, Event as CEvent, KeyCode, KeyEvent},
    execute,
    terminal::{enable_raw_mode, disable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use tokio::sync::broadcast::Receiver;
use std::{io, sync::{Arc, Mutex}, time::Duration};
use tokio::time::sleep;
use ratatui::backend::Backend;

const MAX_TRADES: usize = 200;
const MAX_FILLS: usize = 200;
const MAX_ORDERS: usize = 500;
const FEE_RATE: f64 = 0.001; // 0.1% per side

pub async fn run_tui(
    mut evt_rx: Receiver<MarketEvent>,
    mut fill_rx: Receiver<Fill>,
    mut order_rx: Receiver<OrderIntent>,
) -> Result<()> {
    // caches (shared with background collectors)
    let trades = Arc::new(Mutex::new(Vec::<TradeEvent>::new()));
    let fills = Arc::new(Mutex::new(Vec::<Fill>::new()));
    let top_book = Arc::new(Mutex::new(None::<BookEvent>));
    let orders = Arc::new(Mutex::new(Vec::<OrderIntent>::new()));

    // background: market events (trades + book)
    {
        let trades = trades.clone();
        let top_book = top_book.clone();
        tokio::spawn(async move {
            while let Ok(ev) = evt_rx.recv().await {
                match ev {
                    MarketEvent::Trade(t) => {
                        let mut tv = trades.lock().unwrap();
                        tv.push(t);
                        if tv.len() > MAX_TRADES { tv.remove(0); }
                    }
                    MarketEvent::Book(b) => {
                        let mut tb = top_book.lock().unwrap();
                        *tb = Some(b);
                    }
                    _ => {}
                }
            }
        });
    }

    // fills collector
    {
        let fills = fills.clone();
        tokio::spawn(async move {
            while let Ok(f) = fill_rx.recv().await {
                let mut fv = fills.lock().unwrap();
                fv.push(f);
                if fv.len() > MAX_FILLS { fv.remove(0); }
            }
        });
    }

    // orders collector (broadcast)
    {
        let orders = orders.clone();
        tokio::spawn(async move {
            while let Ok(o) = order_rx.recv().await {
                let mut ov = orders.lock().unwrap();
                ov.push(o);
                if ov.len() > MAX_ORDERS { ov.remove(0); }
            }
        });
    }

    // terminal init
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // main UI loop
    loop {
        // draw UI
        {
            let tv = trades.lock().unwrap();
            let fv = fills.lock().unwrap();
            let ov = orders.lock().unwrap();
            let tb = top_book.lock().unwrap();
            terminal.draw(|f| {
                draw_ui(f, &tv, &fv, &ov, tb.as_ref());
            })?;
        }

        // input handling with non-blocking poll
        let timeout = Duration::from_millis(100);
        if crossterm::event::poll(timeout)? {
            if let CEvent::Key(KeyEvent{code, ..}) = event::read()? {
                match code {
                    KeyCode::Char('q') => {
                        // on quit: compute and print final report, then exit loop
                        let tv = trades.lock().unwrap().clone();
                        let fv = fills.lock().unwrap().clone();
                        let ov = orders.lock().unwrap().clone();
                        // restore terminal before printing long text
                        disable_raw_mode()?;
                        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                        terminal.show_cursor()?;
                        print_report(&ov, &fv);
                        return Ok(());
                    }
                    KeyCode::Char('r') => {
                        // on 'r' print a quick report to stdout (non-intrusive)
                        let ov = orders.lock().unwrap().clone();
                        let fv = fills.lock().unwrap().clone();
                        print_report_compact(&ov, &fv);
                    }
                    KeyCode::Char(' ') => { /* toggle pause - not implemented */ }
                    _ => {}
                }
            }
        }

        sleep(Duration::from_millis(50)).await;
    }
}

/// core UI drawing using ratatui widgets
fn draw_ui(f: &mut Frame<'_>, trades: &Vec<TradeEvent>, fills: &Vec<Fill>, orders: &Vec<OrderIntent>, top_book: Option<&BookEvent>) {
    let size = f.size();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Length(3), Constraint::Percentage(55), Constraint::Length(10)].as_ref())
        .split(size);

    // header (show top-of-book if present)
    let title = match top_book {
        Some(b) => format!("Simulator TUI — {} | bid={:?}({:?}) ask={:?}({:?})",
        b.ts, b.best_bid_price, b.best_bid_qty, b.best_ask_price, b.best_ask_qty),
        None => format!("Simulator TUI — no book"),
    };
    let header = Paragraph::new(title).block(Block::default().borders(Borders::ALL).title("Header"));
    f.render_widget(header, chunks[0]);

    // middle: trades and fills table
    let mid_chunks = Layout::default().direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)].as_ref())
        .split(chunks[1]);

    // trades table (latest 20)
    let rows: Vec<Row> = trades.iter().rev().take(20).map(|t| {
        Row::new(vec![
            Cell::from(t.s.clone()),
            Cell::from(t.p.clone()),
            Cell::from(t.q.clone()),
            Cell::from(t.E.to_string()),
        ])
    }).collect();

    let widths = vec![Constraint::Length(12), Constraint::Length(12), Constraint::Length(12), Constraint::Length(12)];
    let table = Table::new(rows, widths).block(Block::default().borders(Borders::ALL).title("Trades"));
    f.render_widget(table, mid_chunks[0]);

    // fills & orders pane
    let mut lines = vec![];
    lines.push(format!("Recent fills (latest {}):", fills.len().min(8)));
    for fll in fills.iter().rev().take(8) {
        lines.push(format!("{} {} @ {:.2} qty={:.6}", &fll.order_id[..8.min(fll.order_id.len())], fll.symbol, fll.price, fll.qty));
    }
    lines.push("---- Orders (latest) ----".to_string());
    for ord in orders.iter().rev().take(8) {
        lines.push(format!("{} {} {} qty={:.6}", &ord.id[..8.min(ord.id.len())], ord.symbol, ord.side, ord.qty));
    }
    let para = Paragraph::new(lines.join("\n")).block(Block::default().borders(Borders::ALL).title("Fills / Orders")).wrap(Wrap { trim: true });
    f.render_widget(para, mid_chunks[1]);

    // bottom: controls + quick stats
    let stats = format!("Controls: q=quit+report, r=print compact report, space=play/pause | fills={} orders={}", fills.len(), orders.len());
    let hints = Paragraph::new(stats).block(Block::default().borders(Borders::ALL).title("Controls / Stats"));
    f.render_widget(hints, chunks[2]);
}

/// Compact on-demand report (prints short summary)
fn print_report_compact(orders: &Vec<OrderIntent>, fills: &Vec<Fill>) {
    let (closed_trades, total_pnl, wins, losses) = build_trade_report(orders, fills);
    println!("----- Compact report -----");
    println!("Closed trades: {}  Total PnL: {:.6}", closed_trades.len(), total_pnl);
    println!("Wins: {}  Losses: {}", wins, losses);
    if !closed_trades.is_empty() {
        println!("Last closed trade: entry={} exit={} qty={} pnl={:.6}", 
            closed_trades.last().unwrap().entry_id, closed_trades.last().unwrap().exit_id, closed_trades.last().unwrap().qty, closed_trades.last().unwrap().pnl);
    }
    println!("--------------------------");
}

/// Full report printed at exit
fn print_report(orders: &Vec<OrderIntent>, fills: &Vec<Fill>) {
    let (closed_trades, total_pnl, wins, losses) = build_trade_report(orders, fills);

    println!();
    println!("================ FINAL REPORT ================");
    println!("Closed trades: {}", closed_trades.len());
    println!("Total realized PnL: {:.8}", total_pnl);
    println!("Wins: {}  Losses: {}  Win rate: {:.2}%", wins, losses, 
        if closed_trades.is_empty() { 0.0 } else { 100.0 * (wins as f64) / (closed_trades.len() as f64) });
    // max drawdown on cumulative pnl timeline
    let mut cum = 0.0;
    let mut peak = std::f64::NEG_INFINITY;
    let mut max_dd = 0.0;
    for ct in &closed_trades {
        cum += ct.pnl;
        if cum > peak { peak = cum; }
        let dd = peak - cum;
        if dd > max_dd { max_dd = dd; }
    }
    println!("Max drawdown (realized sequence): {:.8}", max_dd);

    println!();
    println!("Per-trade summary (most recent 40):");
    for t in closed_trades.iter().rev().take(40) {
        println!(
            "{} -> {}  qty={:.6} entry={:.6} exit={:.6} pnl={:.8}",
            &t.entry_id[..8.min(t.entry_id.len())],
            &t.exit_id[..8.min(t.exit_id.len())],
            t.qty, t.entry_price, t.exit_price, t.pnl
        );
    }
    println!("==============================================");
}

/// Closed trade record returned by FIFO matcher
struct ClosedTrade {
    entry_id: String,
    exit_id: String,
    entry_price: f64,
    exit_price: f64,
    qty: f64,
    pnl: f64,
}

/// Build closed trades using FIFO matching between fills and orders.
/// Returns (closed_trades, total_realized_pnl, wins, losses)
fn build_trade_report(orders: &Vec<OrderIntent>, fills: &Vec<Fill>) -> (Vec<ClosedTrade>, f64, usize, usize) {
    use std::collections::VecDeque;

    // map order_id -> OrderIntent for side lookup
    let mut order_map = std::collections::HashMap::new();
    for o in orders {
        order_map.insert(o.id.clone(), o.clone());
    }

    // We'll maintain a FIFO list of open lots (symbol-agnostic here; you can extend per-symbol).
    // Each lot: (qty_signed, price, order_id)
    // buy fills => positive qty, sell fills => negative qty
    let mut open_lots: VecDeque<(f64, f64, String)> = VecDeque::new();
    let mut closed: Vec<ClosedTrade> = Vec::new();

    for f in fills {
        let order = order_map.get(&f.order_id);
        // if order missing, skip
        let side = order.map(|o| o.side.as_str()).unwrap_or("");
        let fill_qty = f.qty;
        let fill_price = f.price;
        // signed qty: buy => +qty, sell => -qty
        let signed_qty = match side {
            "buy" | "Buy" | "BUY" => fill_qty,
            "sell" | "Sell" | "SELL" => -fill_qty,
            _ => {
                // unknown: infer by sign of qty (assume positive => buy)
                fill_qty
            }
        };

        // apply fee per side (deduct from pnl when closing). We'll account fee at match time.
        let mut remaining = signed_qty;
        if remaining == 0.0 { continue; }

        // if same sign as tail (i.e., adding to existing direction), just push as new open lot
        if open_lots.is_empty() || open_lots.back().map(|(q,_,_)| q.signum()) == Some(remaining.signum()) {
            open_lots.push_back((remaining.abs() * remaining.signum(), fill_price, f.order_id.clone()));
            continue;
        }

        // Otherwise match against opposite-signed open lots FIFO
        while remaining.abs() > 0.0 && !open_lots.is_empty() {
            let mut front = open_lots.pop_front().unwrap();
            let front_qty = front.0; // signed
                                     // if front and remaining are opposite sign, match
            if front_qty.signum() == remaining.signum() {
                // same sign (shouldn't happen because handled above), push back and break
                open_lots.push_front(front);
                break;
            }
            let match_qty = remaining.abs().min(front_qty.abs());
            // determine entry & exit: entry is the lot that was earlier (front), exit is current fill if signs opposite
            let (entry_price, exit_price, entry_id, exit_id, qty_matched) = if front_qty > 0.0 && remaining < 0.0 {
                // front was buy, current is sell => closing long
                (front.1, fill_price, front.2.clone(), f.order_id.clone(), match_qty)
            } else if front_qty < 0.0 && remaining > 0.0 {
                // front was short (negative), current is buy => closing short
                (front.1, fill_price, front.2.clone(), f.order_id.clone(), match_qty)
            } else {
                // shouldn't reach
                (front.1, fill_price, front.2.clone(), f.order_id.clone(), match_qty)
            };

            // fees: both sides pay fee on their side when executed; compute fees on both entry & exit
            let fee_entry = entry_price * qty_matched * FEE_RATE;
            let fee_exit = exit_price * qty_matched * FEE_RATE;

            // pnl: for long-close: (exit - entry) * qty - fees_total
            let pnl = (exit_price - entry_price) * qty_matched - (fee_entry + fee_exit);

            closed.push(ClosedTrade {
                entry_id,
                exit_id,
                entry_price,
                exit_price,
                qty: qty_matched,
                pnl,
            });

            // reduce remaining and front lot
            remaining = if remaining.abs() > match_qty {
                // subtract matched qty keeping sign
                let sign = remaining.signum();
                (remaining.abs() - match_qty) * sign
            } else {
                0.0
            };

            let front_remaining = if front_qty.abs() > match_qty {
                let sign = front_qty.signum();
                (front_qty.abs() - match_qty) * sign
            } else {
                0.0
            };

            if front_remaining.abs() > 0.0 {
                // push leftover front back to front of queue
                open_lots.push_front((front_remaining, front.1, front.2));
            }
        }

        // if any remaining after matching, push it as open lot
        if remaining.abs() > 0.0 {
            open_lots.push_back((remaining, fill_price, f.order_id.clone()));
        }
    }

    // aggregate
    let total_pnl: f64 = closed.iter().map(|c| c.pnl).sum();
    let wins = closed.iter().filter(|c| c.pnl > 0.0).count();
    let losses = closed.iter().filter(|c| c.pnl <= 0.0).count();

    (closed, total_pnl, wins, losses)
}
