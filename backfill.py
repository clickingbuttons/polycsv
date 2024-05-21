import sys
import gzip
import csv
import pathlib
from polygon import RESTClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import date, timedelta, datetime
import argparse

client = RESTClient(retries=20, num_pools=1)
fieldnames = [
    'ticker',
    'ticker_root',
    'ticker_suffix',
    'active',
    'name',
    'primary_exchange',
    'list_date',
    'delisted_utc',
    'description',
    'homepage_url',
    # address
    'address1',
    'address2',
    'city',
    'state',
    'country',
    'postal_code',
    # branding
    'icon_url',
    'logo_url',
    'accent_color',
    'light_color',
    'dark_color',

    'cik',
    'composite_figi',
    'phone_number',
    'share_class_figi',
    'share_class_shares_outstanding',
    'sic_code',
    'sic_description',
    'total_employees',
    'weighted_shares_outstanding',
]
fmt = "%Y-%m-%d"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from', default='2003-09-10')
    parser.add_argument('--to', default=date.today().strftime(fmt))
    parser.add_argument('-o', '--outdir', default='tickers')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    start = datetime.strptime(args.__getattribute__('from'), fmt)
    end = datetime.strptime(args.to, fmt)
    executor = ThreadPoolExecutor(max_workers=5)

    pathlib.Path(args.outdir).mkdir(parents=True, exist_ok=True)
    futures = [executor.submit(day, args.outdir, d.strftime(fmt), args.force) for d in daterange(start, end)]
    for f in as_completed(futures):
        print(f.result())

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        res = start_date + timedelta(n)
        if res.weekday() < 5:
            yield res

def flatten(obj, member):
    if not obj[member]:
        return
    for k, v in obj[member].items():
        obj[k] = v

def day(outdir: str, d: str, force: bool):
    path = f"{outdir}/{d}.csv.gz"
    if not force and pathlib.Path(path).exists():
        return f"{d} (skipped)"

    gz = gzip.open(path, 'wt')
    writer = csv.DictWriter(gz, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    tickers = client.list_tickers(date=d, market='stocks', limit=1000, sort=None, order=None)

    executor = ThreadPoolExecutor(max_workers=50)
    futures = []

    for t in sorted(tickers, key=lambda t: t.ticker):
        kwargs = { "ticker": t.ticker, "date": d }
        future = executor.submit(client.get_ticker_details, **kwargs)
        futures.append(future)

    for f in futures:
        try:
            data = f.result()
        except Exception as exc:
            print(exc)
            sys.exit(1)
        else:
            data2 = asdict(data)
            flatten(data2, 'address')
            flatten(data2, 'branding')
            writer.writerow(data2)

    gz.close()
    return d

if __name__ == '__main__':
    main()
