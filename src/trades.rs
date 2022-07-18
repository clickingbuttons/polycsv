use chrono::NaiveDate;
use polygon_io::{client::Client as PolygonClient, equities::trades::Trade};
use std::{
  fs::File,
  io::ErrorKind,
  process,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex
  }
};
use threadpool::ThreadPool;

pub fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  polygon: &mut PolygonClient,
  path: &str,
  tickers: Vec<String>
) -> usize {
  let trades = Arc::new(Mutex::new(Vec::<Trade>::new()));
  let counter = Arc::new(AtomicUsize::new(0));
  let num_tickers = tickers.len();
  for t in tickers.iter() {
    let day_format = date.clone();
    let t = t.clone();
    let trades_day = Arc::clone(&trades);
    let mut client = polygon.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 20 times
      for j in 0..20 {
        match client.get_all_trades(&t, date) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            trades_day.lock().unwrap().append(&mut resp);
            counter.fetch_add(1, Ordering::Relaxed);
            println!(
              "\x1b[1A\x1b[Ktrades : {:5} / {:5} [{}]",
              counter.load(Ordering::Relaxed),
              num_tickers,
              t
            );
            return;
          }
          Err(e) => match e.kind() {
            ErrorKind::UnexpectedEof => {
              eprintln!("\x1b[1A\x1b[K{}: no trades {}\n", day_format, t);
              return;
            }
            _ => {
              eprintln!(
                "\x1b[1A\x1b[K{} {}: get_trades retry {}: {}\n",
                day_format,
                t,
                j + 1,
                e.to_string()
              );
              std::thread::sleep(std::time::Duration::from_secs(j + 1));
            }
          }
        }
      }
      eprintln!("{} {}: failure\n", day_format, t);
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
    let stream = zstd::stream::write::Encoder::new(writer, 0)
      .expect("zstd")
      .auto_finish();
    let mut writer = csv::WriterBuilder::new()
      .delimiter('|' as u8)
      .has_headers(true)
      .from_writer(stream);
    for row in trades.drain(..) {
      writer.serialize(row).expect("serialize");
    }
    writer.flush().expect("flush");

    eprintln!("{}: Done", date);
  });

  return num_trades;
}
