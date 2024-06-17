import logging

import apsw
import apsw.ext

from . import BaseFeed

# https://bsky.app/profile/nora.zone/post/3kv35hqi4a22b
TARGET_QUOTE_URI = 'at://did:plc:4qqizocrnriintskkh6trnzv/app.bsky.feed.post/3kv35hqi4a22b'

class NoraZoneInteresting(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/nz-interesting'

    def __init__(self):
        self.db_cnx = apsw.Connection('db/nz-interesting.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, create_ts timestamp);
            create index if not exists create_ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.nz_interesting')

    def process_commit(self, commit):
        if commit['opType'] != 'c':
            return

        if commit['collection'] != 'app.bsky.feed.post':
            return

        record = commit.get('record')
        if record is None:
            return

        embed = record.get('embed')
        if embed is None:
            return

        inner_record = embed.get('record')
        if inner_record is None:
            return

        if inner_record.get('uri') == TARGET_QUOTE_URI:
            self.logger.debug('found quote post of target, adding to feed')
            uri = 'at://{repo}/app.bsky.feed.post/{rkey}'.format(
                repo = commit['did'],
                rkey = commit['rkey']
            )
            ts = self.safe_timestamp(record.get('createdAt')).timestamp()
            self.db_cnx.execute(
                'insert into posts (uri, create_ts) values (:uri, :ts)',
                dict(uri=uri, ts=ts)
            )

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_feed(self, limit, offset, langs):
        cur = self.db_cnx.execute(
            'select uri from posts order by create_ts desc limit :limit offset :offset',
            dict(limit=limit, offset=offset)
        )
        return [uri for (uri,) in cur]
