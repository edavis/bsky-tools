#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone
from io import BytesIO
import json
import os
import sqlite3
import sys

from atproto import CAR
import redis
import dag_cbor
import websockets

app_bsky_allowlist = set([
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
])

async def bsky_activity():
    relay_url = 'ws://localhost:6008/subscribe'

    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=60) as firehose:
        while True:
            payload = BytesIO(await firehose.recv())

            yield json.load(payload)

async def main():
    redis_cnx = redis.Redis()
    redis_pipe = redis_cnx.pipeline()

    if os.path.exists('/opt/muninsky/users.db'):
        db_fname = '/opt/muninsky/users.db'
    else:
        db_fname = 'users.db'

    db_cnx = sqlite3.connect(db_fname)
    with db_cnx:
        db_cnx.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = off;
        CREATE TABLE IF NOT EXISTS users (did TEXT, ts TIMESTAMP);
        CREATE UNIQUE INDEX IF NOT EXISTS did_idx on users(did);
        CREATE INDEX IF NOT EXISTS ts_idx on users(ts);
        """)

    sys.stdout.write('starting up\n')
    sys.stdout.flush()

    op_count = 0
    async for payload in bsky_activity():
        if payload['opType'] != 'c':
            continue

        collection = payload['collection']
        if collection not in app_bsky_allowlist:
            continue

        repo_did = payload['did']
        repo_update_time = datetime.now(timezone.utc)
        db_cnx.execute(
            'insert into users values (:did, :ts) on conflict (did) do update set ts = :ts',
            {'did': repo_did, 'ts': repo_update_time.timestamp()}
        )

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
            now = datetime.now(timezone.utc)
            payload_seq = payload['seq']
            payload_lag = now - repo_update_time

            sys.stdout.write(f'seq: {payload_seq}, lag: {payload_lag.total_seconds()}\n')
            redis_pipe.set('dev.edavis.muninsky.seq', payload_seq)
            redis_pipe.execute()
            db_cnx.commit()
            sys.stdout.flush()

if __name__ == '__main__':
    asyncio.run(main())
