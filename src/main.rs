mod calendar;
mod tickers;
mod trades;
mod util;

use chrono::{Duration, NaiveDate, Utc};
use polygon_io::{client::Client as PolygonClient, reference::ticker_details::TickerDetail};
use std::{fs::create_dir_all, panic, path::Path, process, time::Instant};
use threadpool::ThreadPool;
use tickers::download_tickers_day;
use trades::download_trades_day;
use util::MarketDays;

fn main() {
  // Holds API key and ratelimit
  let mut polygon = PolygonClient::new();

  // Enough threads to end up blocking on io
  let thread_pool = ThreadPool::new(100);

  // Panic if thread panics
  let orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    orig_hook(panic_info);
    process::exit(1);
  }));

  let data_dir = "data";
  let tickers_dir = format!("{}/tickers", data_dir);
  create_dir_all(&tickers_dir).expect("mkdir");
  let trades_dir = format!("{}/trades", data_dir);
  create_dir_all(&trades_dir).expect("mkdir");

  let start = Instant::now();
  let from = NaiveDate::from_ymd(2004, 1, 1);
  let to = Utc::now().naive_utc().date() - Duration::days(4); // polygon backfills from TAQ after 2 days. We want TAQ.
  let market_days = (MarketDays { from, to }).collect::<Vec<NaiveDate>>();
  for date in market_days.into_iter().rev() {
    println!("{}\n", date);
    let now = Instant::now();
    let tickers_path = format!("{}/{}.csv", tickers_dir, date);
    if Path::new(&tickers_path).exists() {
      eprintln!("{} exists, skipping", tickers_path);
    } else {
      let num_tickers = download_tickers_day(date, &thread_pool, &mut polygon, &tickers_path);
      eprintln!(
        "{}: Downloaded {} tickers in {}s",
        date,
        num_tickers,
        now.elapsed().as_secs()
      );
    }

    let now = Instant::now();
    let trades_path = format!("{}/{}.csv.zst", trades_dir, date);
    if Path::new(&trades_path).exists() {
      eprintln!("{} exists, skipping", trades_path);
    } else {
      println!("");
      eprintln!("{}: Downloading trades", date);
      let mut tickers = Vec::new();
      let mut rdr = csv::Reader::from_path(tickers_path).unwrap();
      for r in rdr.deserialize() {
        let row: TickerDetail = r.expect("ticker detail");
        tickers.push(row.ticker);
      }
      let num_trades = download_trades_day(date, &thread_pool, &mut polygon, &trades_path, tickers);
      eprintln!(
        "{}: Downloaded {} trades in {}s",
        date,
        num_trades,
        now.elapsed().as_secs()
      );
    }
  }

  thread_pool.join();
  eprintln!("Finished in {}s", start.elapsed().as_secs());
}
