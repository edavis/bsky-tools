#!/usr/bin/env python3

import asyncio
from io import BytesIO
import json
import logging
import signal

from atproto import CAR
import dag_cbor
import websockets

from feed_manager import feed_manager

logging.basicConfig(
    format='%(asctime)s - %(levelname)-5s - %(name)-20s - %(message)s',
    level=logging.DEBUG
)
logging.getLogger('').setLevel(logging.WARNING)
logging.getLogger('feeds').setLevel(logging.DEBUG)
logging.getLogger('firehose').setLevel(logging.DEBUG)
logging.getLogger('feedgen').setLevel(logging.DEBUG)

logger = logging.getLogger('feedgen')

async def firehose_events():
    relay_url = 'ws://localhost:6008/subscribe'

    logger = logging.getLogger('feeds.events')
    logger.info(f'opening websocket connection to {relay_url}')

    async with websockets.connect(relay_url, ping_timeout=60) as firehose:
        while True:
            payload = BytesIO(await firehose.recv())
            yield json.load(payload)

async def main():
    event_count = 0

    async for commit in firehose_events():
        feed_manager.process_commit(commit)
        event_count += 1
        if event_count % 2500 == 0:
            feed_manager.commit_changes()

def handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    logger.error(f"Caught exception: {msg}")
    logger.info("Shutting down...")
    asyncio.create_task(shutdown(loop))

async def shutdown(loop, signal=None):
    if signal:
        logger.info(f'received exit signal {signal.name}')
    feed_manager.stop_all()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f'cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    catch_signals = (signal.SIGTERM, signal.SIGINT)
    for sig in catch_signals:
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(loop, signal=s))
        )
    loop.set_exception_handler(handle_exception)

    try:
        loop.create_task(main())
        loop.run_forever()
    finally:
        loop.close()
