import logging

import apsw
import apsw.ext

from . import BaseFeed

class OutlineTagsFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/outline'
    SERVE_FEED_QUERY = """
    select uri, create_ts
    from posts
    order by create_ts desc
    limit :limit offset :offset
    """

    def __init__(self):
        self.db_cnx = apsw.Connection('db/outlinetags.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, create_ts timestamp);
            create unique index if not exists create_ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.outlinetags')

    def process_commit(self, commit):
        if commit['opType'] != 'c':
            return

        if commit['collection'] != 'app.bsky.feed.post':
            return

        record = commit.get('record')
        if record is None:
            return

        if not record.get('tags', []):
            return

        repo = commit['did']
        rkey = commit['rkey']
        post_uri = f'at://{repo}/app.bsky.feed.post/{rkey}'
        ts = self.safe_timestamp(record.get('createdAt')).timestamp()
        self.transaction_begin(self.db_cnx)
        self.db_cnx.execute(
            'insert into posts (uri, create_ts) values (:uri, :ts)',
            dict(uri=post_uri, ts=ts)
        )

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.transaction_commit(self.db_cnx)
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_feed(self, limit, offset, langs):
        cur = self.db_cnx.execute(self.SERVE_FEED_QUERY, dict(limit=limit, offset=offset))
        return [row[0] for row in cur]

    def serve_feed_debug(self, limit, offset, langs):
        bindings = dict(limit=limit, offset=offset)
        return apsw.ext.format_query_table(
            self.db_cnx, self.SERVE_FEED_QUERY, bindings,
            string_sanitize=2, text_width=9999, use_unicode=True
        )
