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
    relay_url = 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
    firehose_seq = redis_cnx.get('bsky-tools:firehose:subscribe-repos:seq')
    if firehose_seq:
        relay_url += f'?cursor={firehose_seq.decode()}'

    sys.stdout.write(f'opening websocket to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
        current_minute = None
        while True:
            message = BytesIO(await firehose.recv())
            header = dag_cbor.decode(message, allow_concat=True)
            if header['op'] != 1:
                continue

            redis_cnx.publish('bsky-tools:firehose:stream', message.getvalue())

            # checkpoint the seq
            now = datetime.now(timezone.utc)
            if now.time().minute != current_minute:
                current_minute = now.time().minute

                payload = dag_cbor.decode(message)
                payload_seq = payload['seq']
                payload_time = datetime.strptime(payload['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                payload_lag = now - payload_time

                redis_cnx.set('bsky-tools:firehose:subscribe-repos:seq', payload_seq)
                sys.stdout.write(f'seq: {payload_seq}, lag: {payload_lag.total_seconds()}\n')
                sys.stdout.flush()

if __name__ == '__main__':
    asyncio.run(main())
