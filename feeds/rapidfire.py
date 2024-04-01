import sqlite3

MAX_TEXT_LENGTH = 140

class RapidFireFeed:
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/rapidfire'

    def __init__(self):
        self.checkpoint = 0
        self.db_cnx = sqlite3.connect('db/rapidfire.db')
        with self.db_cnx:
            self.db_cnx.executescript("""
            pragma journal_mode = WAL;
            pragma synchronous = off;
            create table if not exists posts (uri text, create_ts timestamp);
            create index if not exists create_ts_idx on posts(create_ts);
            """)

    def process(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.post':
            return

        ts = commit['time']
        record = op['record']

        if all([
            len(record['text']) <= MAX_TEXT_LENGTH,
            all(0x20 <= ord(c) <= 0x7e for c in record['text']),
            record.get('reply') is None,
            record.get('embed') is None,
            record.get('facets') is None
        ]):
            repo = commit['repo']
            path = op['path']
            post_uri = f'at://{repo}/{path}'
            self.db_cnx.execute(
                'insert into posts (uri, create_ts) values (:uri, :ts)',
                dict(uri=post_uri, ts=ts)
            )

            self.checkpoint += 1
            if self.checkpoint % 10 == 0:
                self.db_cnx.execute("delete from posts where strftime('%s', create_ts) < strftime('%s', 'now', '-1 hour')")
                self.db_cnx.commit()

    def serve(self, limit, offset):
        cur = self.db_cnx.execute(
            "select uri from posts order by create_ts desc limit :limit offset :offset",
            dict(limit=limit, offset=offset)
        )
        return [uri for (uri,) in cur]
