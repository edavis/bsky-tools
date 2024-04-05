import logging

import apsw
import apsw.ext
import grapheme

from . import BaseFeed

class BattleFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/battle'

    def __init__(self):
        self.db_cnx = apsw.Connection('db/battle.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (
                uri text,
                grapheme_length integer unique,
                create_ts timestamp,
                lang text
            );
            """)

        self.logger = logging.getLogger('feeds.battle')

    def process_commit(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.post':
            return

        record = op.get('record')
        if record is None:
            return

        repo = commit['repo']
        path = op['path']
        post_uri = f'at://{repo}/{path}'
        l = grapheme.length(record['text'])
        ts = self.safe_timestamp(record['createdAt']).timestamp()

        self.transaction_begin(self.db_cnx)

        langs = record.get('langs') or ['']
        for lang in langs:
            self.db_cnx.execute("""
            insert into posts(uri, grapheme_length, create_ts, lang)
            values(:uri, :length, :ts, :lang)
            on conflict(grapheme_length) do update set uri = :uri
            """, dict(uri=post_uri, length=l, ts=ts, lang=lang))

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.transaction_commit(self.db_cnx)
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_feed(self, limit, offset, langs):
        # if '*' in langs:
        #     cur = self.db_cnx.execute(
        #         "select uri from posts order by grapheme_length asc limit :limit offset :offset",
        #         dict(limit=limit, offset=offset)
        #     )
        #     return [uri for (uri,) in cur]
        # else:
        #     lang_values = list(langs.values())
        #     lang_selects = ['select uri, grapheme_length from posts where lang = ?'] * len(lang_values)
        #     lang_clause = ' union '.join(lang_selects)
        #     cur = self.db_cnx.execute(
        #         lang_clause + ' order by grapheme_length asc limit ? offset ?',
        #         [*lang_values, limit, offset]
        #     )
        #     return [uri for (uri, create_ts) in cur]

        cur = self.db_cnx.execute("""
        select uri
        from posts
        order by grapheme_length asc
        limit :limit offset :offset
        """, dict(limit=limit, offset=offset))
        return [uri for (uri,) in cur]

    def serve_feed_debug(self, limit, offset, langs):
        query = """
        select *
        from posts
        order by grapheme_length asc
        limit :limit offset :offset
        """
        bindings = dict(limit=limit, offset=offset)
        return apsw.ext.format_query_table(
            self.db_cnx, query, bindings,
            string_sanitize=2, text_width=9999, use_unicode=True
        )
