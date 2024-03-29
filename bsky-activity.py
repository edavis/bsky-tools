#!/usr/bin/env python3

import dag_cbor
import os
import redis
import sqlite3
import sys
from datetime import datetime, timezone
from firehose_utils import commit_ops
from io import BytesIO

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

def main():
    redis_cnx = redis.Redis()
    redis_pipe = redis_cnx.pipeline()
    redis_sub = redis_cnx.pubsub(ignore_subscribe_messages=True)

    db_fname = '/opt/muninsky/users.db'
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

    op_count = 0
    redis_sub.subscribe('bsky-tools:firehose:stream')
    for event in redis_sub.listen():
        frame = BytesIO(event['data'])
        header = dag_cbor.decode(frame, allow_concat=True)
        if header['op'] != 1 or header['t'] != '#commit':
            continue

        payload = dag_cbor.decode(frame)
        if payload['tooBig']:
            # TODO(ejd): how handle these?
            continue

        for op in commit_ops(payload):
            if op['action'] != 'create':
                continue

            collection, _ = op['path'].split('/')
            if collection not in app_bsky_allowlist:
                continue

            repo_did = payload['repo']
            ts = datetime.now(timezone.utc).timestamp()
            db_cnx.execute(
                'insert into users values (:did, :ts) on conflict (did) do update set ts = :ts',
                {'did': repo_did, 'ts': ts}
            )

            redis_pipe \
                .incr(collection) \
                .incr('dev.edavis.muninsky.ops')

            op_count += 1
            if op_count % 500 == 0:
                payload_seq = payload['seq']
                sys.stdout.write(f'checkpoint: seq: {payload_seq}\n')
                redis_pipe.set('dev.edavis.muninsky.seq', payload_seq)
                redis_pipe.execute()
                db_cnx.commit()
                sys.stdout.flush()

if __name__ == '__main__':
    main()
