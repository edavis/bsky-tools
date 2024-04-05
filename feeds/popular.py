import logging

import apsw
import apsw.ext

from . import BaseFeed

class PopularFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/popular'

    def __init__(self):
        super().__init__()

        self.db_cnx = apsw.Connection('db/popular.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, create_ts timestamp, update_ts timestamp, temperature int);
            create unique index if not exists uri_idx on posts(uri);
            """)

        self.logger = logging.getLogger('feeds.popular')

    def process_commit(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.like':
            return

        record = op['record']
        if record is None:
            return

        ts = self.safe_timestamp(record['createdAt']).timestamp()
        like_subject_uri = op['record']['subject']['uri']

        self.transaction_begin(self.db_cnx)

        self.db_cnx.execute("""
        insert into posts (uri, create_ts, update_ts, temperature)
        values (:uri, :ts, :ts, 1)
        on conflict (uri) do update set temperature = temperature + 1, update_ts = :ts
        """, dict(uri=like_subject_uri, ts=ts))

    def delete_old_posts(self):
        self.db_cnx.execute("""
        delete from posts
        where
            temperature * exp( -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 ) ) < 1.0
            and create_ts < unixepoch('now', '-15 minutes')
        """)

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.delete_old_posts()
        self.transaction_commit(self.db_cnx)
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_feed(self, limit, offset, langs):
        cur = self.db_cnx.execute("""
        select uri from posts
        order by temperature * exp( -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 ) )
        desc limit :limit offset :offset
        """, dict(limit=limit, offset=offset))
        return [uri for (uri,) in cur]

    def serve_feed_debug(self, limit, offset, langs):
        query = """
        select
            uri, temperature,
            unixepoch('now') - create_ts as age_seconds,
            exp(
              -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 )
            ) as decay,
            temperature * exp(
              -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 )
            ) as score
        from posts
        order by score desc
        limit :limit offset :offset
        """
        bindings = dict(limit=limit, offset=offset)
        return apsw.ext.format_query_table(
            self.db_cnx, query, bindings,
            string_sanitize=2, text_width=9999, use_unicode=True
        )
