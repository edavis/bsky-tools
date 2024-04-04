#!/usr/bin/env python3

import asyncio
import dag_cbor
import logging
import sys
import websockets

from atproto import CAR
from io import BytesIO
from datetime import datetime, timezone

from feeds import FeedManager
from feeds.rapidfire import RapidFireFeed
from feeds.popular import PopularFeed
from firehose_manager import FirehoseManager

logging.basicConfig(
    format='%(asctime)s - %(levelname)-5s - %(name)-20s - %(message)s',
    level=logging.DEBUG
)
logging.getLogger('').setLevel(logging.WARNING)
logging.getLogger('feeds').setLevel(logging.DEBUG)

async def firehose_events(firehose_manager):
    relay_url = 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
    seq = firehose_manager.get_sequence_number()
    if seq:
        relay_url += f'?cursor={seq}'

    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
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

async def main():
    firehose_manager = FirehoseManager()

    feed_manager = FeedManager()
    feed_manager.register(RapidFireFeed)
    # feed_manager.register(PopularFeed)

    current_minute = None
    async for commit in firehose_events(firehose_manager):
        feed_manager.process_commit(commit)

        now = datetime.now(timezone.utc)
        if now.minute != current_minute:
            current_minute = now.minute
            feed_manager.run_tasks_minute()
            firehose_manager.set_sequence_number(commit['seq'])

if __name__ == '__main__':
    asyncio.run(main())
