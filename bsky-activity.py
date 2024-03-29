#!/usr/bin/env python3

import os
import redis
import sqlite3
import sys
from datetime import datetime, timezone
from firehose_utils import subscribe_commits

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
    for commit, op in subscribe_commits():
        if op['action'] != 'create':
            continue

        collection, _ = op['path'].split('/')
        if collection not in app_bsky_allowlist:
            continue

        repo_did = commit['repo']
        now = datetime.now(timezone.utc)
        db_cnx.execute(
            'insert into users values (:did, :ts) on conflict (did) do update set ts = :ts',
            {'did': repo_did, 'ts': now.timestamp()}
        )

        redis_pipe \
            .incr(collection) \
            .incr('dev.edavis.muninsky.ops')

        op_count += 1
        if op_count % 1000 == 0:
            payload_seq = commit['seq']
            payload_time = datetime.strptime(commit['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            payload_lag = now - payload_time

            sys.stdout.write(f'seq: {payload_seq}, lag: {payload_lag.total_seconds()}\n')
            redis_pipe.set('dev.edavis.muninsky.seq', payload_seq)
            redis_pipe.execute()
            db_cnx.commit()
            sys.stdout.flush()

if __name__ == '__main__':
    main()
