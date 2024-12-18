#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone
import json
import os
import sqlite3
import sys

import redis
import websockets

app_bsky_allowlist = (
    'app.bsky.actor.profile',
    'app.bsky.feed.generator',
    'app.bsky.feed.like',
    'app.bsky.feed.post',
    'app.bsky.feed.postgate',
    'app.bsky.feed.repost',
    'app.bsky.feed.threadgate',
    'app.bsky.graph.block',
    'app.bsky.graph.follow',
    'app.bsky.graph.list',
    'app.bsky.graph.listblock',
    'app.bsky.graph.listitem',
    'app.bsky.graph.starterpack',
    'app.bsky.labeler.service',
    'chat.bsky.actor.declaration',
)

other_allowlist = (
    'social.psky',
    'blue.zio.atfile',
    'com.shinolabs.pinksea',
    'com.whtwnd',
    'events.smokesignal',
    'fyi.unravel',
    'xyz.statusphere',
)

async def bsky_activity():
    relay_url = 'wss://jetstream1.us-west.bsky.network/subscribe'

    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=60) as firehose:
        while True:
            yield json.loads(await firehose.recv())

async def main():
    redis_cnx = redis.Redis()
    redis_pipe = redis_cnx.pipeline()

    sys.stdout.write('starting up\n')
    sys.stdout.flush()

    op_count = 0
    async for event in bsky_activity():
        if event['kind'] != 'commit':
            continue

        payload = event.get('commit')
        if payload is None:
            continue

        if payload['operation'] != 'create':
            continue

        collection = payload['collection']
        if not collection.startswith(app_bsky_allowlist + other_allowlist):
            continue

        for prefix in other_allowlist:
            if collection.startswith(prefix):
                redis_pipe.incr('dev.edavis.atproto.collection.' + prefix.replace('.', '_'))

        if collection == 'app.bsky.feed.post':
            embed = payload['record'].get('embed')
            if embed is not None and embed.get('$type', ''):
                embed_type = embed['$type']
                redis_pipe.incr(f'app.bsky.feed.post:embed:{embed_type}')

        redis_pipe \
            .incr(collection) \
            .incr('dev.edavis.muninsky.ops')

        op_count += 1
        if op_count % 500 == 0:
            event_time_ms = event['time_us'] / 1_000_000
            sys.stdout.write(f'timestamp: {event_time_ms}\n')
            redis_pipe.execute()
            sys.stdout.flush()

if __name__ == '__main__':
    asyncio.run(main())
