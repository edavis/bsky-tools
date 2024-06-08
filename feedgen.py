#!/usr/bin/env python3

import asyncio
from io import BytesIO
import json
import logging

from atproto import CAR
import dag_cbor
import websockets

from feed_manager import feed_manager
from firehose_manager import FirehoseManager

logging.basicConfig(
    format='%(asctime)s - %(levelname)-5s - %(name)-20s - %(message)s',
    level=logging.DEBUG
)
logging.getLogger('').setLevel(logging.WARNING)
logging.getLogger('feeds').setLevel(logging.DEBUG)
logging.getLogger('firehose').setLevel(logging.DEBUG)

async def firehose_events(firehose_manager):
    relay_url = 'ws://localhost:6008/subscribe'

    logger = logging.getLogger('feeds.events')
    logger.info(f'opening websocket connection to {relay_url}')

    async with websockets.connect(relay_url, ping_timeout=60) as firehose:
        while True:
            payload = BytesIO(await firehose.recv())
            yield json.load(payload)

async def main():
    firehose_manager = FirehoseManager()
    event_count = 0

    async for commit in firehose_events(firehose_manager):
        feed_manager.process_commit(commit)
        event_count += 1
        if event_count % 2500 == 0:
            feed_manager.commit_changes()
            firehose_manager.set_sequence_number(commit['seq'])

if __name__ == '__main__':
    asyncio.run(main())
