import math
import sqlite3

class PopularFeed:
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/popular'

    def __init__(self):
        self.checkpoint = 0
        self.db_cnx = sqlite3.connect('db/popular.db')
        self.db_cnx.create_function('exp', 1, math.exp)
        with self.db_cnx:
            self.db_cnx.executescript(
                "pragma journal_mode = WAL;"
                "pragma synchronous = off;"
                "create table if not exists posts (uri text, create_ts timestamp, update_ts timestamp, temperature int);"
                "create unique index if not exists uri_idx on posts(uri);"
            )

    def process(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.like':
            return

        ts = commit['time']
        like_subject = op['record']['subject']['uri']

        self.db_cnx.execute((
            "insert into posts (uri, create_ts, update_ts, temperature) "
            "values (:uri, :ts, :ts, 1) "
            "on conflict (uri) do update set temperature = temperature + 1, update_ts = :ts"
        ), dict(uri=like_subject, ts=ts))

        self.checkpoint += 1
        if self.checkpoint % 100 == 0:
            self.db_cnx.execute("delete from posts where strftime('%s', create_ts) < strftime('%s', 'now', '-3 hours')")
            self.db_cnx.commit()

    def serve(self, limit, offset):
        cur = self.db_cnx.execute((
            "select uri from posts "
            "order by temperature * exp( "
            "-1 * ( ( strftime( '%s', 'now' ) - strftime( '%s', create_ts ) ) / 1800.0 ) "
            ") desc limit :limit offset :offset"
        ), dict(limit=limit, offset=offset))
        return [uri for (uri,) in cur]
