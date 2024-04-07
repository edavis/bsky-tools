#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone
from io import BytesIO
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
    'app.bsky.feed.repost',
    'app.bsky.feed.threadgate',
    'app.bsky.graph.block',
    'app.bsky.graph.follow',
    'app.bsky.graph.list',
    'app.bsky.graph.listblock',
    'app.bsky.graph.listitem',
    'app.bsky.labeler.service',
])

async def bsky_activity():
    redis_cnx = redis.Redis()
    relay_url = 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
    firehose_seq = redis_cnx.get('dev.edavis.muninsky.seq')
    if firehose_seq:
        relay_url += f'?cursor={firehose_seq.decode()}'

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
                # TODO(ejd): figure out how to get blocks out-of-band
                continue

            # TODO(ejd): figure out how to validate blocks
            blocks = payload.pop('blocks')
            car_parsed = dict(blocks = {})

            message = payload.copy()
            del message['ops']
            message['commit'] = message['commit'].encode('base32')

            for commit_op in payload['ops']:
                op = commit_op.copy()
                if op['cid'] is not None:
                    op['cid'] = op['cid'].encode('base32')
                    # op['record'] = car_parsed.blocks.get(op['cid'])

                yield message, op

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
    async for commit, op in bsky_activity():
        if op['action'] != 'create':
            continue

        collection, _ = op['path'].split('/')
        if collection not in app_bsky_allowlist:
            continue

        repo_did = commit['repo']
        repo_update_time = datetime.strptime(commit['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        db_cnx.execute(
            'insert into users values (:did, :ts) on conflict (did) do update set ts = :ts',
            {'did': repo_did, 'ts': repo_update_time.timestamp()}
        )

        redis_pipe \
            .incr(collection) \
            .incr('dev.edavis.muninsky.ops')

        op_count += 1
        if op_count % 500 == 0:
            now = datetime.now(timezone.utc)
            payload_seq = commit['seq']
            payload_lag = now - repo_update_time

            sys.stdout.write(f'seq: {payload_seq}, lag: {payload_lag.total_seconds()}\n')
            redis_pipe.set('dev.edavis.muninsky.seq', payload_seq)
            redis_pipe.execute()
            db_cnx.commit()
            sys.stdout.flush()

if __name__ == '__main__':
    asyncio.run(main())
