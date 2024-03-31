#!/usr/bin/env python3

import asyncio
import dag_cbor
import redis
import sys
import websockets
from atproto import CAR
from io import BytesIO

from feeds import Manager
from feeds.rapidfire import RapidFireFeed

async def firehose_events():
    redis_cnx = redis.Redis()
    relay_url = 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
    firehose_seq = redis_cnx.get('dev.edavis.feedgen.seq')
    if firehose_seq:
        relay_url += f'?cursor={firehose_seq.decode()}'

    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
        op_count = 0
        while True:
            frame = BytesIO(await firehose.recv())
            header = dag_cbor.decode(frame, allow_concat=True)
            if header['op'] != 1 or header['t'] != '#commit':
                continue

            payload = dag_cbor.decode(frame)
            if payload['tooBig']:
                continue

            blocks = payload.pop('blocks')
            car_parsed = CAR.from_bytes(blocks)
            message = payload.copy()
            del message['ops']
            message['commit'] = message['commit'].encode('base32')

            for op in payload['ops']:
                repo_op = op.copy()
                if op['cid'] is not None:
                    repo_op['cid'] = repo_op['cid'].encode('base32')
                    repo_op['record'] = car_parsed.blocks[repo_op['cid']]
                message['op'] = repo_op
                yield message

            op_count += 1
            if op_count % 500 == 0:
                redis_cnx.set('dev.edavis.feedgen.seq', payload['seq'])

async def main():
    manager = Manager()
    manager.register(RapidFireFeed)

    async for commit in firehose_events():
        manager.process(commit)


if __name__ == '__main__':
    asyncio.run(main())
