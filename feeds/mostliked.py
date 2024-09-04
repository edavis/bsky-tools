import logging

import apsw
import apsw.ext

from . import BaseFeed

class MostLikedFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/most-liked'
    SERVE_FEED_QUERY = """
    select uri, create_ts, unixepoch('now', '-24 hours'), create_ts - unixepoch('now', '-24 hours'), likes
    from posts
    where create_ts >= unixepoch('now', '-24 hours')
    order by likes desc, create_ts asc
    limit :limit offset :offset
    """
    DELETE_OLD_POSTS_QUERY = """
    delete from posts
    where create_ts < unixepoch('now', '-24 hours')
    """

    def __init__(self):
        self.db_cnx = apsw.Connection('db/mostliked.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (
              uri text, create_ts timestamp, likes int
            );
            create unique index if not exists uri_idx on posts(uri);
            create index if not exists create_ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.mostliked')

    def process_commit(self, commit):
        return

    def delete_old_posts(self):
        self.db_cnx.execute(self.DELETE_OLD_POSTS_QUERY)
        self.logger.debug('deleted {} old posts'.format(self.db_cnx.changes()))

    def commit_changes(self):
        self.delete_old_posts()
        self.logger.debug('committing changes')
        self.transaction_commit(self.db_cnx)
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_feed(self, limit, offset, langs):
        return [
            'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.post/3l3cgg5vbc72i'
        ]

    def serve_feed_debug(self, limit, offset, langs):
        pass
