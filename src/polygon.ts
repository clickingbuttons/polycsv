import { RateLimiter } from 'limiter-es6-compat';
import { Logger } from 'pino';

export function ymd(date: Date) {
	return date.toISOString().substring(0, 10);
}

export type GroupedDaily = {
	T: string,
	v: number,
	vw: number,
	o: number,
	c: number,
	h: number,
	l: number,
	t: number,
	n: number,
};

export type TickerDetail = {
	ticker: string,
	name?: string,
	market?: string,
	locale?: string,
	primary_exchange?: string,
	type?: string,
	active?: string,
	currency_symbol?: string,
	currency_name?: string,
	base_currency_symbol?: string,
	base_currency_name?: string,
	cusip?: string,
	cik?: string,
	composite_figi?: string,
	share_class_figi?: string,
	last_updated_utc?: string,
	delisted_utc?: string,
	phone_number?: string,
	address1?: string,
	address2?: string,
	city?: string,
	state?: string,
	country?: string,
	postal_code?: string,
	description?: string,
	sic_code?: string,
	sic_description?: string,
	ticker_root?: string,
	ticker_suffix?: string,
	homepage_url?: string,
	total_employees?: number,
	list_date?: string,
	logo_url?: string,
	icon_url?: string,
	accent_color?: string,
	light_color?: string,
	dark_color?: string,
	share_class_shares_outstanding?: number,
	weighted_shares_outstanding?: number,
	is_test?: boolean,
	unit_of_trade?: number,
	round_lot?: number,
};

export type Sink = NodeJS.WritableStream;

export class Polygon {
	static base = 'https://api.polygon.io';
	headers: { [k: string]: string };
	limiter: any;

	constructor(public logger: Logger, apiKey?: string) {
		if (!apiKey) apiKey = process.env.POLYGON_KEY;
		if (!apiKey) throw new Error('no api key provided or in POLYGON_KEY');
		this.headers = {
			Authorization: `Bearer ${apiKey}`,
		};
		this.limiter = new RateLimiter({ tokensPerInterval: 400, interval: 'second' });
	}

	async retryFetch(url: string, init: RequestInit, tries = 6) {
		let controller = new AbortController();
		let attempt = 0;

		while (attempt <= tries) {
			try {
				const signal = controller.signal;
				setTimeout(() => controller.abort(), 5000);

				const response = await fetch(url, { ...init, signal: signal });
				if (response.status == 200 || response.status == 404) {
					return response;
				} else {
					if (response.status == 429) this.limiter.removeTokens(this.limiter.getTokensRemaining());
					throw new Error(`Request failed with status: ${response.status}`);
				}
			} catch (e) {
				if (attempt === tries) {
					controller.abort();
					throw e;
				} else {
					attempt++;
					const waitTime = attempt * attempt;
					this.logger.warn(`${url} ${attempt + 1}/${tries + 1} failed. Retrying in ${waitTime} seconds.`);
					this.logger.warn(e, url);

					await new Promise(resolve => setTimeout(resolve, waitTime * 1000));

					controller = new AbortController();
				}
			}
		}

		controller.abort();
		throw new Error(`All ${tries + 1} attempts failed.`);
	}

	async fetchCSV(url: string, sink: Sink): Promise<void> {
		let nextUrl = `${Polygon.base}${url}`;
		while (nextUrl) {
			await this.limiter.removeTokens(1);
			const resp = await this.retryFetch(nextUrl, {
				headers: {
					...this.headers,
					Accept: 'text/csv',
				},
			});
			if (resp.status == 200) {
				const text = await resp.text();
				sink.write(text);
				const maybeNext = resp.headers.get('link');
				nextUrl = maybeNext?.substring(1, maybeNext?.indexOf('>')) ?? '';
			} else if (resp.status == 404) {
				return;
			} else {
				throw new Error(url + ' responded with ' + resp.status);
			}
		}
	}

	async fetchJSON(url: string) {
		let nextUrl = `${Polygon.base}${url}`;
		await this.limiter.removeTokens(1);
		const resp = await this.retryFetch(nextUrl, { headers: this.headers });
		if (resp.status == 200) {
			const data = await resp.json();
			return data.results;
		} else if (resp.status == 404) {
			return null;
		} else {
			throw new Error('url ' + resp.status);
		}
	}

	async groupedDaily(date: Date): Promise<GroupedDaily[]> {
		return this.fetchJSON(`/v2/aggs/grouped/locale/us/market/stocks/${ymd(date)}`) ?? [];
	}

	async tickerDetail(ticker: string, date: Date): Promise<TickerDetail> {
		return this.fetchJSON(`/v3/reference/tickers/${ticker}?date=${ymd(date)}`);
	}

	async trades(ticker: string, date: Date, sink: Sink): Promise<void> {
		return this.fetchCSV(`/v3/trades/${ticker}?timestamp=${ymd(date)}&limit=50000`, sink);
	}
}
