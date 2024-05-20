import re
from datetime import date
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
from typing import List

class Context:
    def __init__(self, fname: str):
        ticker_suffix = "p[A-Z]?|(\\.WS)?\\.[A-Z]|p?\\.WD|\\.[A-Z]|p?\\.[A-Z]?CL|p[A-Z]w|\\.EC|\\.PP||\\.CV||\\.[A-Z]CV|p[A-Z]\\.(CV|WD)|r|\\.U|r?p?w|\\.Aw|\\.WSw";
        self.regexes = []
        with open(fname, 'r') as f:
            start = date(1970, 1, 1)
            end = date(3000, 1, 1)
            for line in f:
                if line.startswith(';!'):
                    kv = line.replace(';!', '').strip().split('=')
                    ymd = [int(i) for i in kv[1].split('-')]
                    if kv[0] == 'start':
                        start = date(ymd[0], ymd[1], ymd[2])
                    elif kv[0] == 'end':
                        end = date(ymd[0], ymd[1], ymd[2])
                if line.startswith(';'):
                    continue
                self.regexes.append({
                   "start": start,
                   "end": end,
                   "regex": re.compile(f"^{line.strip()}({ticker_suffix})?$")
               })
                start = date(1970, 1, 1)
                end = date(3000, 1, 1)

    def is_test(self, ticker: str, d: date):
        for r in self.regexes:
            if d < r['start'] or d > r['end']:
                continue
            if r['regex'].match(ticker):
                return True
        return False

def handle_msg(msg: List[WebSocketMessage]):
    global ctx
    now = date.today()
    for m in msg:
        if ctx.is_test(m.symbol, now):
            continue;
        conditions = m.conditions or []
        conditions = '"' + ','.join(map(str, conditions)) + '"' if len(conditions) > 1 else ','.join(map(str, conditions))
        row = [
            m.symbol,
            conditions,
            0, # correction
            m.exchange,
            m.id if m.id is not None else 0,
            '', # participant_timestamp
            m.price if m.price is not None else 0,
            m.sequence_number,
            m.timestamp,
            m.size if m.size is not None else 0,
            m.tape,
            m.trf_id if m.trf_id is not None else 0,
            m.trf_timestamp if m.trf_timestamp is not None else 0,
        ]
        print(','.join(map(str, row)))


if __name__ == '__main__':
    ctx = Context("./test_tickers.txt")
    ws = WebSocketClient(subscriptions=["T.*"])
    print('ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp')
    ws.run(handle_msg=handle_msg)
