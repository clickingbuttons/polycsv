mod tickers;
mod trades;

use clap::Parser;
use indicatif::MultiProgress;
use log::{info, warn};
use polygon_io::{client::Client as PolygonClient, reference::ticker_details::TickerDetail};
use std::{
	cmp::Ordering,
	fs::{create_dir_all, File},
	io::Read,
	panic,
	path::{Path, PathBuf},
	process,
	time::Instant
};
use threadpool::ThreadPool;
use tickers::{download_tickers_day, list_tickers_day};
use time::{macros::format_description, Date, Duration, OffsetDateTime, UtcOffset, Weekday};
use trades::download_trades_day;

// polygon backfills from TAQ after 2 days
fn tplus3() -> String {
	OffsetDateTime::now_utc()
		.date()
		.checked_sub(Duration::days(3))
		.unwrap()
		.to_string()
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
	#[arg(short, long, default_value = "data")]
	data_dir: PathBuf,
	#[arg(short, long, default_value = "2003-09-10")]
	from:     String,
	#[arg(short, long, default_value_t = tplus3())]
	to:       String
}

fn read_tickers(path: &PathBuf) -> Vec<String> {
	let mut res = Vec::new();
	if !Path::new(&path).exists() {
		return res;
	}

	let stream = File::open(&path).unwrap();
	let mut rdr = zstd::stream::read::Decoder::new(stream).unwrap();

	// Check first for partially written file
	let mut buf = Vec::<u8>::new();
	match rdr.read_to_end(&mut buf) {
		Ok(_) => {
			let mut rdr = csv::ReaderBuilder::new()
				.delimiter(b'|')
				.from_reader(buf.as_slice());
			for r in rdr.deserialize() {
				let row: TickerDetail = r.expect("ticker detail");
				res.push(row.ticker);
			}
		}
		Err(e) => {
			warn!("err reading {:?}: {}", path, e);
		}
	};

	res
}

fn valid_trades_file(path: &PathBuf) -> bool {
	if !Path::new(&path).exists() {
		return false;
	}

	// TODO: find way to quickly tell if zstd file has premature end
	true
}

fn setup_logger() {
	let offset = UtcOffset::local_offset_at(OffsetDateTime::UNIX_EPOCH).unwrap();
	let now = OffsetDateTime::now_utc().to_offset(offset);
	let format = format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]");
	let log_path = format!("{}.log", now.format(format).unwrap());

	fern::Dispatch::new()
		.format(move |out, message, record| {
			let now = OffsetDateTime::now_utc().to_offset(offset);
			let format = format_description!("[[[year]-[month]-[day]][[[hour]:[minute]:[second]]");
			out.finish(format_args!(
				"{}[{}][{}] {}",
				now.format(format).unwrap(),
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

// TODO: proper market calendar to prevent listing tickers on holiday
fn is_market_open(date: &Date) -> bool {
	let weekday = date.weekday();
	weekday != Weekday::Saturday && weekday != Weekday::Sunday
}

fn download_day(date: &str, tickers_dir: &PathBuf, trades_dir: &PathBuf) {
	let date = date.to_string();
	let progress = MultiProgress::new();

	let day_pool = ThreadPool::new(2);
	let tickers_path = tickers_dir.join(format!("{}.csv.zst", date));
	let mut tickers = read_tickers(&tickers_path);
	if tickers.len() == 0 {
		let mut polygon = PolygonClient::new().unwrap();
		let pool = ThreadPool::new(polygon.get_ratelimit() as usize);
		tickers = list_tickers_day(&mut polygon, &date);
		let progress = progress.clone();
		let tickers = tickers.clone();
		let date = date.clone();
		day_pool.execute(move || {
			let now = Instant::now();
			download_tickers_day(&pool, &mut polygon, progress, &date, &tickers_path, tickers);
			info!(
				"Downloaded {:?} in {}s",
				tickers_path,
				now.elapsed().as_secs()
			);
		});
	}

	// Download highly traded tickers first to prevent waiting for pagination at the end
	let highly_traded = vec![
		"AAPL", "TSLA", "SPY", "SQQQ", "TQQQ", "NVDA", "AMD", "QQQ", "META", "MSFT", "GOOGL",
		"SOXL", "GOOG", "BABA", "NIO", "XLE", "DIS", "VOO",
	];
	tickers.sort_unstable_by(|a, b| {
		if highly_traded.contains(&a.as_str()) {
			if highly_traded.contains(&b.as_str()) {
				return Ordering::Equal;
			}
			return Ordering::Less;
		}
		if highly_traded.contains(&b.as_str()) {
			return Ordering::Greater;
		}

		return a.partial_cmp(b).unwrap();
	});

	let trades_path = trades_dir.join(format!("{}.csv.zst", date));
	if !valid_trades_file(&trades_path) {
		let mut polygon = PolygonClient::new().unwrap();
		let pool = ThreadPool::new(polygon.get_ratelimit() as usize);
		let progress = progress.clone();
		let tickers = tickers.clone();
		day_pool.execute(move || {
			let now = Instant::now();
			download_trades_day(&pool, &mut polygon, progress, &date, &trades_path, tickers);
			info!(
				"Downloaded {:?} in {}s",
				trades_path,
				now.elapsed().as_secs()
			);
		});
	}
	day_pool.join();
}

fn main() {
	let args = Cli::parse();
	setup_logger();

	// Panic if thread panics
	let orig_hook = panic::take_hook();
	panic::set_hook(Box::new(move |panic_info| {
		orig_hook(panic_info);
		process::exit(1);
	}));

	let tickers_dir = args.data_dir.join("tickers");
	create_dir_all(&tickers_dir).expect("mkdir");
	let trades_dir = args.data_dir.join("trades");
	create_dir_all(&trades_dir).expect("mkdir");

	let format = format_description!("[year]-[month]-[day]");
	let from = time::Date::parse(&args.from, &format).unwrap();
	let to = time::Date::parse(&args.to, &format).unwrap();
	eprintln!("ingesting from {} to {}", from, to);

	let start = Instant::now();
	// most recent days first
	let mut date = to.clone();
	while date >= from {
		date -= Duration::days(1);
		if !is_market_open(&date) {
			continue;
		}
		download_day(&date.to_string(), &tickers_dir, &trades_dir);
	}

	eprintln!("Finished in {}s", start.elapsed().as_secs());
}
