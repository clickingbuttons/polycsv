use chrono::NaiveDate;
use csv;
use linya::Progress;
use log::{error, warn};
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
  sync::{Arc, Mutex}
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
  progress: Arc<Mutex<Progress>>,
  path: &str
) -> Vec<String> {
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

  let n = tickers.len();
  let msg = format!("Downloading ticker details for {}", date);
  let bar = Arc::new(Mutex::new(progress.lock().unwrap().bar(n, msg)));
  let ticker_details = Arc::new(Mutex::new(Vec::<TickerDetail>::with_capacity(n)));

  for t in tickers {
    let tickers_details = Arc::clone(&ticker_details);
    let mut client = polygon.clone();
    let progress = progress.clone();
    let bar = bar.clone();
    thread_pool.execute(move || {
      // Retry up to 10 times
      for j in 0..10 {
        let params = TickerDetailsParams::new()
          .date(&date.format("%Y-%m-%d").to_string())
          .params;
        match client.get_ticker_details(&t, Some(&params)) {
          Ok(result) => {
            tickers_details.lock().unwrap().push(result.results);
            progress
              .lock()
              .unwrap()
              .inc_and_draw(&bar.lock().unwrap(), 1);
            return;
          }
          Err(e) => {
            // TODO: real errors in polygon_io
            if e.to_string().contains("status: 404") {
              warn!("no details for {} on {}", t, date);
              return;
            }
            warn!(
              "get_ticker_details for {} on {} retry {}: {}",
              t,
              date,
              j + 1,
              e.to_string()
            );
            std::thread::sleep(std::time::Duration::from_secs(j + 1));
          }
        }
      }
      error!("failed downloading ticker details for {} on {}", t, date);
      process::exit(1);
    });
  }
  thread_pool.join();

  let mut tickers_details = ticker_details.lock().unwrap();
  tickers_details.sort_unstable_by(|c1, c2| c1.ticker.cmp(&c2.ticker));
  let writer = File::create(&path).expect("file create");
  let mut writer = csv::Writer::from_writer(writer);
  for row in tickers_details.iter() {
    writer.serialize(row).expect("serialize");
  }
  writer.flush().expect("flush");

  tickers_details.iter().map(|d| d.ticker.clone()).collect()
}
