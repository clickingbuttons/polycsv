use csv;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use log::{error, warn};
use polygon_io::{
	client::Client as PolygonClient,
	core::grouped::{GroupedParams, Locale, Market},
	reference::ticker_details::{TickerDetail, TickerDetailsParams}
};
use serde::{Deserialize, Serialize};
use std::{
	collections::HashSet,
	fmt::Write,
	fs::File,
	path::PathBuf,
	process,
	sync::{Arc, Mutex}
};
use threadpool::ThreadPool;

#[derive(Debug, Deserialize, Serialize)]
pub struct Ticker {
	#[serde(flatten)]
	pub detail: TickerDetail,
	pub day:    String
}

pub fn list_tickers_day(polygon: &mut PolygonClient, date: &str) -> Vec<String> {
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
	tickers.iter().map(|d| d.clone()).collect()
}

pub fn download_tickers_day(
	thread_pool: &ThreadPool,
	polygon: &mut PolygonClient,
	progress: MultiProgress,
	date: &str,
	path: &PathBuf,
	tickers: Vec<String>
) {
	let n_tickers = tickers.len() as u64;
	let bar = progress.add(ProgressBar::new(n_tickers));
	let template = "{date} tickers [{elapsed_precise}] [{wide_bar}] {msg:<8} {pos:>5}/{len:5}";
	let date2 = date.to_string();
	let style = ProgressStyle::with_template(template)
		.unwrap()
		.with_key("date", move |_state: &ProgressState, w: &mut dyn Write| {
			write!(w, "{}", date2).unwrap()
		});
	bar.set_style(style.clone());

	let file = File::create(&path).unwrap();
	let mut zstd = zstd::stream::write::Encoder::new(file, 1).unwrap();
	zstd.multithread(4).unwrap();
	let zstd = zstd.auto_finish();
	let csv = csv::WriterBuilder::new()
		.delimiter(b'|' as u8)
		.has_headers(true)
		.from_writer(zstd);
	let writer = Arc::new(Mutex::new(csv));

	for t in tickers {
		let writer = Arc::clone(&writer);
		let mut client = polygon.clone();
		let bar = bar.clone();
		let date = date.to_string();
		thread_pool.execute(move || {
			// Retry up to 10 times
			for j in 0..10 {
				let params = TickerDetailsParams::new().date(&date).params;
				match client.get_ticker_details(&t, Some(&params)) {
					Ok(result) => {
						writer
							.lock()
							.unwrap()
							.serialize(result.results)
							.expect("serialize");
						bar.set_message(t);
						bar.inc(1);
						return;
					}
					Err(e) => {
						// TODO: real error types in polygon_io
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
	writer.lock().unwrap().flush().unwrap();
	bar.finish();
}
