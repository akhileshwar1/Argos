// src/tui.rs
use crate::core::*;
use anyhow::Result;
use chrono::Utc;
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph, Wrap, Table, Row, Cell},
    layout::{Constraint, Direction, Layout},
    Terminal, Frame,
    style::{Style, Modifier},
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

pub async fn run_tui(
    mut evt_rx: Receiver<MarketEvent>,
    mut fill_rx: Receiver<Fill>,
    mut order_rx: Receiver<OrderIntent>,
) -> Result<()> {
    // caches
    let trades = Arc::new(Mutex::new(Vec::<TradeEvent>::new()));
    let fills = Arc::new(Mutex::new(Vec::<Fill>::new()));
    let top_book = Arc::new(Mutex::new(None::<BookEvent>));
    let orders = Arc::new(Mutex::new(Vec::<OrderIntent>::new()));

    // spawn background collector for market events (trades + book)
    {
        let trades = trades.clone();
        let top_book = top_book.clone();
        tokio::spawn(async move {
            while let Ok(ev) = evt_rx.recv().await {
                match ev {
                    MarketEvent::Trade(t) => {
                        let mut tv = trades.lock().unwrap();
                        tv.push(t);
                        if tv.len() > 200 { tv.remove(0); }
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
                if fv.len() > 200 { fv.remove(0); }
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
                if ov.len() > 500 { ov.remove(0); }
            }
        });
    }

    // terminal init
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        // draw UI
        {
            // borrow caches for rendering
            let tv = trades.lock().unwrap();
            let fv = fills.lock().unwrap();
            let ov = orders.lock().unwrap();
            let tb = top_book.lock().unwrap();
            terminal.draw(|f| {
                draw_ui(f, &tv, &fv, &ov, tb.as_ref());
            })?;
        }

        // input handling
        let timeout = Duration::from_millis(100);
        if crossterm::event::poll(timeout)? {
            if let CEvent::Key(KeyEvent{code, ..}) = event::read()? {
                match code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char(' ') => { /* toggle pause - not implemented */ }
                    _ => {}
                }
            }
        }

        sleep(Duration::from_millis(50)).await;
    }

    // restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

// Note: Frame<'a> in this ratatui version has only a lifetime parameter (no Backend generic).
fn draw_ui(f: &mut Frame<'_>, trades: &Vec<TradeEvent>, fills: &Vec<Fill>, orders: &Vec<OrderIntent>, top_book: Option<&BookEvent>) {
    let size = f.size();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Length(3), Constraint::Percentage(50), Constraint::Length(10)].as_ref())
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

    // widths for table columns
    let widths = vec![Constraint::Length(12), Constraint::Length(12), Constraint::Length(12), Constraint::Length(12)];
    // Table::new(rows, widths) required by this ratatui version
    let table = Table::new(rows, widths).block(Block::default().borders(Borders::ALL).title("Trades"));
    f.render_widget(table, mid_chunks[0]);

    // fills & orders pane
    let mut lines = vec![];
    for fll in fills.iter().rev().take(8) {
        lines.push(format!("Fill {} {}@{:.2} qty={:.6}", fll.order_id, fll.symbol, fll.price, fll.qty));
    }
    lines.push("---- Orders (latest) ----".to_string());
    for ord in orders.iter().rev().take(8) {
        lines.push(format!("Ord {} {} {} qty={:.6}", &ord.id[..8], ord.symbol, ord.side, ord.qty));
    }
    let para = Paragraph::new(lines.join("\n")).block(Block::default().borders(Borders::ALL).title("Fills / Orders")).wrap(Wrap { trim: true });
    f.render_widget(para, mid_chunks[1]);

    // bottom: controls
    let hints = Paragraph::new("Controls: q=quit, space=play/pause").block(Block::default().borders(Borders::ALL).title("Controls"));
    f.render_widget(hints, chunks[2]);
}
