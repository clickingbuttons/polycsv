import csv
import sys
from dataclasses import asdict
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
from typing import List

fieldnames = [
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
    'trf_timestamp',
]
fmt = "%Y-%m-%d"

def handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        m.conditions = m.conditions or []
        m.conditions = ','.join(map(str, m.conditions)) # avoid wrapping in []
        data = asdict(m)
        data['ticker'] = data['symbol']
        writer.writerow(data)

if __name__ == '__main__':
    ws = WebSocketClient(subscriptions=["T.*"])

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, extrasaction='ignore')
    ws.run(handle_msg=handle_msg)
