import os
import apsw
import logging

from . import BaseFeed

MAX_TEXT_LENGTH = 140

class RapidFireFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/rapidfire'

    def __init__(self):
        db_fname = ''
        if os.path.isdir('/dev/shm/'):
            os.makedirs('/dev/shm/feedgens/', exist_ok=True)
            db_fname = '/dev/shm/feedgens/rapidfire.db'
        else:
            db_fname = 'db/rapidfire.db'

        self.db_cnx = apsw.Connection(db_fname)
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('synchronous', 'OFF')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, create_ts timestamp, lang text);
            create index if not exists create_ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.rapidfire')

    def process_commit(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.post':
            return

        record = op['record']

        if all([
            len(record['text']) <= MAX_TEXT_LENGTH,
            record.get('reply') is None,
            record.get('embed') is None,
            record.get('facets') is None
        ]):
            repo = commit['repo']
            path = op['path']
            post_uri = f'at://{repo}/{path}'
            ts = record['createdAt']

            with self.db_cnx:
                langs = record.get('langs') or ['']
                for lang in langs:
                    self.db_cnx.execute(
                        'insert into posts (uri, create_ts, lang) values (:uri, :ts, :lang)',
                        dict(uri=post_uri, ts=ts, lang=lang)
                    )

    def run_tasks_minute(self):
        self.logger.debug('running minute tasks')

        with self.db_cnx:
            self.db_cnx.execute(
                "delete from posts where strftime('%s', create_ts) < strftime('%s', 'now', '-15 minutes')"
            )

        self.db_cnx.pragma('wal_checkpoint(TRUNCATE)')

    def serve_feed(self, limit, offset, langs):
        if '*' in langs:
            cur = self.db_cnx.execute(
                "select uri from posts order by create_ts desc limit :limit offset :offset",
                dict(limit=limit, offset=offset)
            )
            return [uri for (uri,) in cur]
        else:
            lang_values = list(langs.values())
            lang_selects = ['select uri, create_ts from posts where lang = ?'] * len(lang_values)
            lang_clause = ' union '.join(lang_selects)
            cur = self.db_cnx.execute(
                lang_clause + ' order by create_ts desc limit ? offset ?',
                [*lang_values, limit, offset]
            )
            return [uri for (uri, create_ts) in cur]
