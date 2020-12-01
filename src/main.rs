mod agg1d;
mod agg1m;
mod trades;
mod util;

use polygon_io::client::Client;
use std::{panic, process, sync::Arc};
use threadpool::ThreadPool;
// use chrono::NaiveDate;
// use ::zdb::table::Table;

use agg1d::download_agg1d;
use agg1m::download_agg1m;
use trades::download_trades;

fn main() {
  let config = Arc::new(Client::new());
  // Polygon starts throttling after 100 open connections.
  // We want to saturate all 100.
  let thread_pool = ThreadPool::new(150);
  // Panic if thread panics
  let orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    orig_hook(panic_info);
    process::exit(1);
  }));

  // let agg1d =
  //   Table::open("agg1d").expect("Table agg1d must exist to load symbols to download in agg1m");
  // agg1d.scan(
  //   0,
  //   NaiveDate::from_ymd(2222, 02, 01).and_hms(0, 0, 0).timestamp_nanos(),
  //   vec!["ts", "sym"],
  //   |row| {
  //     if row[1].get_symbol() == "DHCP.WS.A" {
  //       println!("{}", row[0].get_timestamp());
  //     }
  //   }
  // );

  println!("Downloading agg1d index data");
  download_agg1d(&thread_pool, config.clone());
  println!("Downloading agg1m data");
  download_agg1m(&thread_pool, config.clone());
  println!("Downloading trade data");
  download_trades(&thread_pool, config.clone());
}
