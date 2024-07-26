import logging

import apsw
import apsw.ext

from . import BaseFeed

class RatioFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/ratio'
    SERVE_FEED_QUERY = """
    with served as (
      select
        uri,
        create_ts,
        ( unixepoch('now') - create_ts ) as age_seconds,
        replies,
        quoteposts,
        likes,
        reposts,
        ( replies + quoteposts ) / ( likes + reposts ) as ratio,
        exp( -1 * ( ( unixepoch('now') - create_ts ) / ( 3600.0 * 16 ) ) ) as decay
      from posts
    )
    select
      *,
      ( ratio * decay ) as score
    from served
    where replies > 15 and ratio > 2.5
    order by score desc
    limit :limit offset :offset
    """
    DELETE_OLD_POSTS_QUERY = """
    delete from posts
    where
      create_ts < unixepoch('now', '-5 days')
    """

    def __init__(self):
        self.db_cnx = apsw.Connection('db/ratio.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (
              uri text, create_ts timestamp,
              replies float, likes float, reposts float, quoteposts float
            );
            create unique index if not exists uri_idx on posts(uri);
            """)

        self.logger = logging.getLogger('feeds.ratio')

    def process_commit(self, commit):
        if commit['opType'] != 'c':
            return

        subject_uri = None
        is_reply = False
        is_quotepost = False

        if commit['collection'] in {'app.bsky.feed.like', 'app.bsky.feed.repost'}:
            record = commit.get('record')
            ts = self.safe_timestamp(record.get('createdAt')).timestamp()
            try:
                subject_uri = record['subject']['uri']
            except KeyError:
                return
        elif commit['collection'] == 'app.bsky.feed.post':
            record = commit.get('record')
            ts = self.safe_timestamp(record.get('createdAt')).timestamp()
            if record.get('reply') is not None:
                is_reply = True
                try:
                    subject_uri = record['reply']['parent']['uri']
                except KeyError:
                    return

                # only count non-OP replies
                if subject_uri.startswith('at://' + commit['did']):
                    return

            elif record.get('embed') is not None:
                is_quotepost = True
                t = record['embed']['$type']
                if t == 'app.bsky.embed.record':
                    try:
                        subject_uri = record['embed']['record']['uri']
                    except KeyError:
                        return
                elif t == 'app.bsky.embed.recordWithMedia':
                    try:
                        subject_uri = record['embed']['record']['record']['uri']
                    except KeyError:
                        return

        if subject_uri is None:
            return

        params = {
            'uri': subject_uri,
            'ts': ts,
            'is_reply': int(is_reply),
            'is_like': int(commit['collection'] == 'app.bsky.feed.like'),
            'is_repost': int(commit['collection'] == 'app.bsky.feed.repost'),
            'is_quotepost': int(is_quotepost),
        }

        self.transaction_begin(self.db_cnx)

        self.db_cnx.execute("""
        insert into posts(uri, create_ts, replies, likes, reposts, quoteposts)
        values (:uri, :ts,
                case when :is_reply then 1 else 0 end,
                case when :is_like then 1 else 0 end,
                case when :is_repost then 1 else 0 end,
                case when :is_quotepost then 1 else 0 end)
        on conflict(uri)
        do update set
          replies = replies + case when :is_reply then 1 else 0 end,
          likes = likes + case when :is_like then 1 else 0 end,
          reposts = reposts + case when :is_repost then 1 else 0 end,
          quoteposts = quoteposts + case when :is_quotepost then 1 else 0 end
        """, params)

    def delete_old_posts(self):
        self.db_cnx.execute(self.DELETE_OLD_POSTS_QUERY)

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.delete_old_posts()
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
