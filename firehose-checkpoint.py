#!/usr/bin/env python3

import asyncio
import dag_cbor
import redis
import sys
import websockets
from datetime import datetime, timezone
from io import BytesIO

async def main():
    redis_cnx = redis.Redis()
    relay_url = 'ws://127.0.0.1:9060'
    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
        current_minute = None
        while True:
            frame = BytesIO(await firehose.recv())
            header = dag_cbor.decode(frame, allow_concat=True)
            if header['op'] != 1:
                continue

            # checkpoint the seq
            now = datetime.now(timezone.utc)
            if now.time().minute != current_minute:
                current_minute = now.time().minute

                payload = dag_cbor.decode(frame)
                payload_seq = payload['seq']
                payload_time = datetime.strptime(payload['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                payload_lag = now - payload_time

                redis_cnx.set('bsky-tools:firehose:subscribe-repos:seq', payload_seq)
                sys.stdout.write(f'seq: {payload_seq}, lag: {payload_lag.total_seconds()}\n')
                sys.stdout.flush()

if __name__ == '__main__':
    asyncio.run(main())
