import logging

import apsw
import apsw.ext

from . import BaseFeed

class PopularQuotePostsFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/popqp'
    SERVE_FEED_QUERY = """
    select uri, create_ts, update_ts, quote_count, exp( -1 * ( ( unixepoch('now') - create_ts ) / 10800.0 ) ) as decay,
    quote_count * exp( -1 * ( ( unixepoch('now') - create_ts ) / 10800.0 ) ) as score
    from posts
    order by quote_count * exp( -1 * ( ( unixepoch('now') - create_ts ) / 10800.0 ) ) desc
    limit :limit offset :offset
    """
    DELETE_OLD_POSTS_QUERY = """
    delete from posts where
      quote_count * exp( -1 * ( ( unixepoch('now') - create_ts ) / 10800.0 ) ) < 1.0
      and create_ts < unixepoch('now', '-24 hours')
    """

    def __init__(self):
        self.db_cnx = apsw.Connection('db/popqp.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (
              uri text, create_ts timestamp, update_ts timestamp, quote_count int
            );
            create unique index if not exists uri_idx on posts(uri);
            """)

        self.logger = logging.getLogger('feeds.popqp')

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

        embed_type = embed.get('$type')
        if embed_type == 'app.bsky.embed.record':
            quote_post_uri = embed['record']['uri']
        elif embed_type == 'app.bsky.embed.recordWithMedia':
            quote_post_uri = embed['record']['record']['uri']
        else:
            return

        ts = self.safe_timestamp(record.get('createdAt')).timestamp()
        self.transaction_begin(self.db_cnx)

        self.db_cnx.execute("""
        insert into posts (uri, create_ts, update_ts, quote_count)
        values (:uri, :ts, :ts, 1)
        on conflict (uri) do
          update set quote_count = quote_count + 1, update_ts = :ts
        """, dict(uri=quote_post_uri, ts=ts))

    def delete_old_posts(self):
        self.db_cnx.execute(self.DELETE_OLD_POSTS_QUERY)
        self.logger.debug('deleted {} old posts'.format(self.db_cnx.changes()))

    def commit_changes(self):
        self.delete_old_posts()
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
