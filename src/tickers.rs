use chrono::NaiveDate;
use csv;
use polygon_io::{
  client::Client as PolygonClient,
  core::grouped::{GroupedParams, Locale, Market},
  helpers::{naive_date_to_string, string_to_naive_date},
  reference::ticker_details::{TickerDetail, TickerDetailsParams}
};
use serde::{Deserialize, Serialize};
use std::{
  collections::HashSet,
  fs::File,
  process,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex
  }
};
use threadpool::ThreadPool;

#[derive(Debug, Deserialize, Serialize)]
pub struct Ticker {
  #[serde(flatten)]
  pub detail: TickerDetail,
  #[serde(
    deserialize_with = "string_to_naive_date",
    serialize_with = "naive_date_to_string"
  )]
  pub day:    NaiveDate
}

pub fn download_tickers_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  polygon: &mut PolygonClient,
  path: &str
) -> usize {
  // Tickers v3 endpoint has pagination problems. I'd rather miss some barely
  // traded tickers than 1000s for a day.
  let mut tickers = HashSet::<String>::default();
  let params = GroupedParams::new().unadjusted(true).params;
  polygon
    .get_grouped(Locale::US, Market::Stocks, date, Some(&params))
    .unwrap()
    .results
    .iter()
    .for_each(|ticker| {
      tickers.insert(ticker.symbol.replace("/", ".").clone());
    });

  eprintln!("{}: Downloading {} tickers", date, tickers.len());
  let ticker_details = Arc::new(Mutex::new(Vec::<TickerDetail>::new()));

  let counter = Arc::new(AtomicUsize::new(0));
  let num_tickers = tickers.len();
  for t in tickers {
    let tickers_details = Arc::clone(&ticker_details);
    let mut client = polygon.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 10 times
      for j in 0..10 {
        let params = TickerDetailsParams::new()
          .date(&date.format("%Y-%m-%d").to_string())
          .params;
        match client.get_ticker_details(&t, Some(&params)) {
          Ok(result) => {
            tickers_details.lock().unwrap().push(result.results);
            counter.fetch_add(1, Ordering::Relaxed);
            println!(
              "\x1b[1A\x1b[Ktickers: {:3} / {} [{}]",
              counter.load(Ordering::Relaxed),
              num_tickers,
              t
            );
            return;
          }
          Err(e) => {
            // TODO: real errors in polygon_io
            if e.to_string().contains("status: 404") {
              eprintln!("\x1b[1A\x1b[K{}: no details {}\n", date, t);
              return;
            }
            eprintln!(
              "\x1b[1A\x1b[K{}: get_ticker_details retry {}: {}\n",
              date,
              j + 1,
              e.to_string()
            );
            std::thread::sleep(std::time::Duration::from_secs(j + 1));
          }
        }
      }
      eprintln!("{}: failure\n", &date);
      process::exit(1);
    });
  }
  thread_pool.join();

  let mut tickers_details = ticker_details.lock().unwrap();
  let num_rows = tickers_details.len();
  tickers_details.sort_unstable_by(|c1, c2| c1.ticker.cmp(&c2.ticker));
  let writer = File::create(&path).expect("file create");
  let mut writer = csv::Writer::from_writer(writer);
  for row in tickers_details.drain(..) {
    writer.serialize(row).expect("serialize");
  }
  writer.flush().expect("flush");

  return num_rows;
}
