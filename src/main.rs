mod calendar;
mod tickers;
mod trades;
mod util;

use chrono::{Duration, Local, NaiveDate, Utc};
use linya::Progress;
use log::{debug, info};
use polygon_io::{client::Client as PolygonClient, reference::ticker_details::TickerDetail};
use std::{
  fs::create_dir_all,
  panic,
  path::Path,
  process,
  sync::{Arc, Mutex},
  time::Instant
};
use threadpool::ThreadPool;
use tickers::download_tickers_day;
use trades::download_trades_day;
use util::MarketDays;

fn download_tickers(
  pool: &ThreadPool,
  polygon: &mut PolygonClient,
  date: NaiveDate,
  progress: Arc<Mutex<Progress>>,
  dir: &str
) -> Vec<String> {
  let now = Instant::now();
  let tickers_path = format!("{}/{}.csv", dir, date);
  let mut tickers = Vec::new();
  if Path::new(&tickers_path).exists() {
    debug!("{} exists, skipping", tickers_path);
    let mut rdr = csv::Reader::from_path(tickers_path).unwrap();
    for r in rdr.deserialize() {
      let row: TickerDetail = r.expect("ticker detail");
      tickers.push(row.ticker);
    }
  } else {
    info!("Downloading tickers for {} to {}", date, tickers_path);
    tickers = download_tickers_day(date, &pool, polygon, progress, &tickers_path);
    info!(
      "Downloaded {} tickers for {} in {}s",
      tickers.len(),
      date,
      now.elapsed().as_secs()
    );
  }

  tickers
}

fn download_trades(
  pool: &ThreadPool,
  polygon: &mut PolygonClient,
  date: NaiveDate,
  progress: Arc<Mutex<Progress>>,
  dir: &str,
  tickers: Vec<String>
) {
  let now = Instant::now();
  let trades_path = format!("{}/{}.csv.zst", dir, date);
  if Path::new(&trades_path).exists() {
    debug!("{} exists, skipping", trades_path);
  } else {
    info!("Downloading trades for {} to {}", date, trades_path);
    let num_trades = download_trades_day(date, &pool, polygon, progress, &trades_path, tickers);
    info!(
      "Downloaded {} trades for {} in {}s",
      num_trades,
      date,
      now.elapsed().as_secs()
    );
  }
}

fn setup_logger() {
  let now = Local::now();
  let log_path = format!("{}.log", now.format("%Y-%m-%d-%H:%M:%S"));
  fern::Dispatch::new()
    .format(|out, message, record| {
      out.finish(format_args!(
        "{}[{}][{}] {}",
        chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
        record.target(),
        record.level(),
        message
      ))
    })
    .level(log::LevelFilter::Info)
    .chain(fern::log_file(log_path).expect("log file"))
    .apply()
    .expect("setup logging");
}

fn main() {
  setup_logger();
  let mut polygon = PolygonClient::new();
  let tickers_pool = ThreadPool::new(200);
  let trades_pool = ThreadPool::new(200);
  let progress = Arc::new(Mutex::new(Progress::new()));

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
  // polygon backfills from TAQ after 2 days
  let to = Utc::now().naive_utc().date() - Duration::days(3);
  // most recent days first
  let market_days = (MarketDays { from, to }).collect::<Vec<NaiveDate>>();
  for date in market_days.into_iter().rev() {
    let tickers = download_tickers(
      &tickers_pool,
      &mut polygon,
      date,
      progress.clone(),
      &tickers_dir
    );

    let mut polygon = polygon.clone();
    let trades_dir = trades_dir.clone();
    let trades_pool = trades_pool.clone();
    let progress = progress.clone();
    tickers_pool.execute(move || {
      download_trades(
        &trades_pool,
        &mut polygon,
        date,
        progress,
        &trades_dir,
        tickers
      );
    });
  }

  tickers_pool.join();
  trades_pool.join();

  eprintln!("Finished in {}s", start.elapsed().as_secs());
}
