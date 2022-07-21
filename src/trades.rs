use chrono::NaiveDate;
use polygon_io::{client::Client as PolygonClient, equities::trades::Trade};
use std::{
  fs::File,
  io::ErrorKind,
  process,
  sync::{
    Arc, Mutex
  }
};
use threadpool::ThreadPool;
use linya::Progress;
use log::{warn, error};

pub fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  polygon: &mut PolygonClient,
  progress: Arc<Mutex<Progress>>,
  path: &str,
  tickers: Vec<String>
) -> usize {
  let trades = Arc::new(Mutex::new(Vec::<Trade>::new()));
  let n = tickers.len();
  let msg = format!("Downloading trades for {}", date);
  let bar = Arc::new(Mutex::new(progress.lock().unwrap().bar(n, msg)));

  for t in tickers.iter() {
    let day_format = date.clone();
    let t = t.clone();
    let trades_day = Arc::clone(&trades);
    let mut client = polygon.clone();
		let progress = progress.clone();
		let bar = bar.clone();
    thread_pool.execute(move || {
      // Retry up to 20 times
      for j in 0..20 {
        match client.get_all_trades(&t, date) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            trades_day.lock().unwrap().append(&mut resp);
						progress.lock().unwrap().inc_and_draw(&bar.lock().unwrap(), 1);
            return;
          }
          Err(e) => match e.kind() {
            ErrorKind::UnexpectedEof => {
              warn!("no trades for {} on {}", t, day_format);
              return;
            }
            _ => {
              warn!(
                "get_trades for {} on {} retry {}: {}",
                t,
                day_format,
                j + 1,
                e.to_string()
              );
              std::thread::sleep(std::time::Duration::from_secs(j + 1));
            }
          }
        }
      }
      error!("failed to download trades for {} on {}", t, day_format);
      process::exit(1);
    });
  }
  thread_pool.join();
  let num_trades = trades.lock().unwrap().len();

  let path = Arc::new(path.to_string());
  thread_pool.execute(move || {
    let mut trades = trades.lock().unwrap();
    trades.sort_unstable_by(|c1, c2| {
      if c1.ticker == c2.ticker {
        c1.time.cmp(&c2.time)
      } else {
        c1.ticker.cmp(&c2.ticker)
      }
    });

    let writer = File::create(&*path).expect("file create");
    let mut stream = zstd::stream::write::Encoder::new(writer, 0)
      .expect("zstd");
    let mut writer = csv::WriterBuilder::new()
      .delimiter('|' as u8)
      .has_headers(true)
      .from_writer(&mut stream);
    for row in trades.drain(..) {
      writer.serialize(row).expect("serialize");
    }
    writer.flush().expect("flush");
		drop(writer);
		stream.finish().expect("flush_zstd");
  });

  return num_trades;
}
