import logging
import re

import apsw
import apsw.ext

from . import BaseFeed

SDW_REGEX = re.compile(r'^(shit|piss|fuck|cunt|cocksucker|motherfucker|tits)[!,./;?~ ]*$', re.I|re.A)

class SevenDirtyWordsFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/sdw'

    def __init__(self):
        self.db_cnx = apsw.Connection('db/sdw.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, create_ts timestamp);
            create unique index if not exists create_ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.sdw')

    def process_commit(self, commit):
        if commit['opType'] != 'c':
            return

        if commit['collection'] != 'app.bsky.feed.post':
            return

        record = commit.get('record')
        if record is None:
            return

        if record.get('reply') is not None:
            return

        # https://en.wikipedia.org/wiki/Seven_dirty_words
        if SDW_REGEX.search(record['text']) is not None:
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
        cur = self.db_cnx.execute("""
        select uri
        from posts
        order by create_ts desc
        limit :limit
        offset :offset
        """, dict(limit=limit, offset=offset))
        return [uri for (uri,) in cur]

    def serve_feed_debug(self, limit, offset, langs):
        query = "select * from posts order by create_ts desc limit :limit offset :offset"
        bindings = dict(limit=limit, offset=offset)
        return apsw.ext.format_query_table(
            self.db_cnx, query, bindings,
            string_sanitize=2, text_width=9999, use_unicode=True
        )
