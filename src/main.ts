import { Downloader } from './downloader.js';
import cliProgress from 'cli-progress';
import pino from 'pino';
import { ymd } from './polygon.js';

async function main() {
	const multibar = new cliProgress.MultiBar({
		// clearOnComplete: true,
		format: ' {bar} | {task} | {value}/{total}',
	});

	const days: Date[] = [];
	const from = new Date(2003, 8, 10);
	const to = new Date(from);
	to.setFullYear(from.getFullYear() + 1);

	for (let d = new Date(from); d <= to; d.setDate(d.getDate() + 1)) {
		if (d.getDay() == 0 || d.getDay() == 6) continue;
		days.push(new Date(d));
	}

	const writeDays = multibar.create(days.length, 0);

	const logger = pino(
		{
			timestamp: () => `,"time":"${new Date(Date.now()).toISOString()}"`
		},
		pino.destination('log.txt')
	);
	const downloader = new Downloader("test_tickers.txt", multibar, logger);

	for (let d of days) {
		writeDays.update({ task: ymd(d) });
		await downloader.day(d);
		writeDays.increment();
	}

	multibar.stop();
}

main();

