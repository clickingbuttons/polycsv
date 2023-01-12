use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use log::{error, warn};
use polygon_io::client::Client as PolygonClient;
use std::{
	fmt::Write,
	fs::File,
	path::PathBuf,
	process,
	sync::{Arc, Mutex}
};
use threadpool::ThreadPool;

pub fn download_trades_day(
	thread_pool: &ThreadPool,
	polygon: &mut PolygonClient,
	progress: MultiProgress,
	date: &str,
	path: &PathBuf,
	tickers: Vec<String>
) {
	let n_tickers = tickers.len() as u64;
	let bar = progress.add(ProgressBar::new(n_tickers));

	let template = "{date} trades  [{elapsed_precise}] [{wide_bar}] {msg:<8} {pos:>5}/{len:5}";
	let date2 = date.to_string();
	let style = ProgressStyle::with_template(template)
		.unwrap()
		.with_key("date", move |_: &ProgressState, w: &mut dyn Write| {
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
			match client.get_all_trades(&t, &date) {
				Ok(resp) => {
					for trade in resp {
						writer.lock().unwrap().serialize(trade).expect("serialize");
					}
					bar.set_message(t);
					bar.inc(1);
					return;
				}
				Err(e) => {
					warn!("get_trades for {} on {}: {}", t, date.clone(), e);
				}
			}
			error!("failed to download trades for {} on {}", t, date.clone());
			process::exit(1);
		});
	}
	thread_pool.join();
	writer.lock().unwrap().flush().unwrap();
	bar.finish();
}
