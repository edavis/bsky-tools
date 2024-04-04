import os
import sys
import math
import sqlite3

from . import BaseFeed

class PopularFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/popular'

    def __init__(self):
        if os.path.isdir('/dev/shm/'):
            os.makedirs('/dev/shm/feedgens/', exist_ok=True)
            self.db_cnx = sqlite3.connect('/dev/shm/feedgens/popular.db')
        else:
            self.db_cnx = sqlite3.connect('db/popular.db')

        self.db_cnx.create_function('exp', 1, math.exp)
        with self.db_cnx:
            self.db_cnx.executescript(
                "pragma journal_mode = WAL;"
                "pragma synchronous = OFF;"
                "pragma wal_autocheckpoint = 0;"
                "create table if not exists posts (uri text, create_ts timestamp, update_ts timestamp, temperature int);"
                "create unique index if not exists uri_idx on posts(uri);"
            )

        self.cleanup_checkpoint = 0

    def process_commit(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.like':
            return

        ts = commit['time']
        like_subject_uri = op['record']['subject']['uri']

        with self.db_cnx:
            self.db_cnx.execute((
                "insert into posts (uri, create_ts, update_ts, temperature) "
                "values (:uri, :ts, :ts, 1) "
                "on conflict (uri) do update set temperature = temperature + 1, update_ts = :ts"
            ), dict(uri=like_subject_uri, ts=ts))

        self.cleanup_checkpoint += 1
        if self.cleanup_checkpoint % 1000 == 0:
            sys.stdout.write('popular: running cleanup checkpoint\n')
            sys.stdout.flush()

            with self.db_cnx:
                self.db_cnx.execute(
                    "delete from posts where temperature * exp( -1 * ( ( strftime( '%s', 'now' ) - strftime( '%s', create_ts ) ) / 1800.0 ) ) < 1.0 and strftime( '%s', create_ts ) < strftime( '%s', 'now', '-15 minutes' )"
                )

            self.db_cnx.execute(
                "pragma wal_checkpoint(TRUNCATE)"
            )

    def serve_feed(self, limit, offset, langs):
        cur = self.db_cnx.execute((
            "select uri from posts "
            "order by temperature * exp( "
            "-1 * ( ( strftime( '%s', 'now' ) - strftime( '%s', create_ts ) ) / 1800.0 ) "
            ") desc limit :limit offset :offset"
        ), dict(limit=limit, offset=offset))
        return [uri for (uri,) in cur]
