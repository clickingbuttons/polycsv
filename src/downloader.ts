import { dirname } from 'node:path';
import { Polygon, ymd, TickerDetail } from './polygon.js';
import { readFileSync, createWriteStream, mkdirSync } from 'node:fs';
import { ZSTDCompress } from 'simple-zstd';
import { Logger } from 'pino';
import { finished } from 'node:stream/promises';
import { Transform, TransformOptions, TransformCallback } from 'node:stream';

class CsvWithHeaderToCsvRows extends Transform {
	columnsCsv: string;
	constructor(columns: string[], public prepend: string, options?: TransformOptions) {
		super(options);
		this.columnsCsv = columns.join(',') + '\n';
	}

	_transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback) {
		let str = chunk.toString();
		if (str.startsWith(this.columnsCsv)) str = str.replace(this.columnsCsv, '');
		const lines = str.split('\n');
		for (let i = 0; i < lines.length; i++) {
			if (lines[i]) {
				this.push(this.prepend);
				this.push(',');
				this.push(lines[i]);
				this.push('\n');
			}
		}
		callback();
	}
}

function numToString(num: number | string): string {
	let sign = "";
	num = num + '';
	num.charAt(0) == "-" && (num = num.substring(1), sign = "-");
	let arr = num.split(/[e]/ig);
	if (arr.length < 2) return sign + num;
	let dot = (.1).toLocaleString().substr(1, 1), n = arr[0], exp = +arr[1],
		w = (n = n.replace(/^0+/, '')).replace(dot, ''),
		pos = n.split(dot)[1] ? n.indexOf(dot) + exp : w.length + exp,
		L   = pos - w.length, s = "" + BigInt(w);
	w   = exp >= 0 ? (L >= 0 ? s + "0".repeat(L) : r()) : (pos <= 0 ? "0" + dot + "0".repeat(Math.abs(pos)) + s : r());
	return sign + w;
	function r() {return w.replace(new RegExp(`^(.{${pos}})(.)`), `$1${dot}$2`)}
}

function writeRow(out: NodeJS.WritableStream, obj: any, keys: string[]) {
	for (let i = 0; i < keys.length; i++) {
		const key = keys[i];
		const value = obj[key] ?? '';
		if (typeof value == 'number') {
			out.write(numToString(value));
		} else {
			out.write(value);
		}
		out.write(',');
	}
	out.write('\n');
}

export class Downloader {
	client: Polygon;
	test_tickers: { [k: string]: undefined };

	constructor(
		testTickerFname: string,
		public multibar: any,
		public logger: Logger,
	) {
		this.client = new Polygon(logger);

		const tickers = readFileSync(testTickerFname, 'utf8');
		this.test_tickers = tickers.split("\n").reduce((acc, cur) => {
			if (cur != "") acc[cur] = undefined;
			return acc;
		}, {} as { [k: string]: undefined });
	}

	writeStream(fname: string, columns: string[]) {
		const dir = dirname(fname);
		mkdirSync(dir, { recursive: true });


		const res = new ZSTDCompress()
		const file = createWriteStream(fname);
		res.pipe(file);
		columns.forEach(c => file.write(c + ','));
		file.write('\n');
		return res;
	}

	async tickerDetails(date: Date, tickers: string[]): Promise<string[]> {
		const filename = `tickers/${ymd(date)}.csv.zst`;
		const columns = [
			'ticker',
			'name',
			'primary_exchange',
			'type',
			'composite_figi',
			'share_class_figi',
			'delisted_utc',
			'city',
			'state',
			'country',
			'description',
			'sic_code',
			'sic_description',
			'ticker_root',
			'ticker_suffix',
			'homepage_url',
			'total_employees',
			'list_date',
			'share_class_shares_outstanding',
			'weighted_shares_outstanding',
			'is_test',
			'unit_of_trade',
			'round_lot',
		] as (keyof TickerDetail)[];
		const out = this.writeStream(filename, columns);

		const bar = this.multibar.create(tickers.length, 0, { task: `write ${filename}` });

		const unknownTestTickers: string[] = [];
		const res: string[] = [];

		const promises = tickers.map(async ticker => {
			try {
				const details = (await this.client.tickerDetail(ticker, date)) ?? { ticker };
				const is_test = details?.is_test ?? false;
				if (is_test) {
					unknownTestTickers.push(details.ticker);
				} else {
					res.push(ticker);
					writeRow(out, details, columns);
				}
			} catch (err) {
				this.logger.error(err, `ticker details ${ticker} on ${ymd(date)}`);
			}

			bar.increment();
		});

		await Promise.all(promises);
		if (unknownTestTickers.length) this.logger.info('unknown test tickers', unknownTestTickers);

		out.end();
		await finished(out);

		return res;
	}

	async trades(date: Date, tickers: string[]) {
		const filename = `trades/${ymd(date)}.csv.zst`;
		const columns = [
			'ticker',
			'conditions',
			'correction',
			'exchange',
			'id',
			'participant_timestamp',
			'price',
			'sequence_number',
			'sip_timestamp',
			'size',
			'tape',
			'trf_id',
			'trf_timestamp'
		];
		const responseColumns = columns.filter(c => c != 'ticker');
		const out = this.writeStream(filename, columns);

		const bar = this.multibar.create(tickers.length, 0, { task: `write ${filename}` });

		const promises = tickers.map(async ticker => {
			try {
				const sink = new CsvWithHeaderToCsvRows(responseColumns, ticker);
				sink.pipe(out);
				await this.client.trades(ticker, date, sink);
				sink.destroy();
			} catch (err) {
				this.logger.error(err, `trades for ${ticker} on ${ymd(date)}`);
			}

			bar.increment();
		});

		await Promise.all(promises);

		out.end();
		await finished(out);
	}

	async day(date: Date) {
		let tickers = (await this.client.groupedDaily(date))
			.map(a => a.T)
			.filter(t => !(t in this.test_tickers))
			.sort();

		tickers = await this.tickerDetails(date, tickers);
		await this.trades(date, tickers);
	}
}
