import logging

import apsw
import apsw.ext

from . import BaseFeed

class PopularFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/popular'

    def __init__(self):
        # use the posts from the most-liked feed for this
        self.db_cnx = apsw.Connection('db/mostliked.db')
        self.db_cnx.pragma('foreign_keys', True)
        self.db_cnx.pragma('journal_mode', 'WAL')

    def process_commit(self, commit):
        pass

    def commit_changes(self):
        pass

    def generate_sql(self, limit, offset, langs):
        bindings = []
        sql = """
        select posts.uri, create_ts, likes, lang, unixepoch('now') - create_ts as age_seconds,
        exp( -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 ) ) as decay,
        likes * exp( -1 * ( ( unixepoch('now') - create_ts ) / 1800.0 ) ) as score
        from posts
        left join langs on posts.uri = langs.uri
        where
        """
        if not '*' in langs:
            lang_values = list(langs.values())
            bindings.extend(lang_values)
            sql += " OR ".join(['lang = ?'] * len(lang_values))
        else:
            sql += " 1=1 "
        sql += """
        order by score desc
        limit ? offset ?
        """
        bindings.extend([limit, offset])
        return sql, bindings

    def serve_feed(self, limit, offset, langs):
        sql, bindings = self.generate_sql(limit, offset, langs)
        cur = self.db_cnx.execute(sql, bindings)
        return [row[0] for row in cur]

    def serve_feed_debug(self, limit, offset, langs):
        sql, bindings = self.generate_sql(limit, offset, langs)
        return apsw.ext.format_query_table(
            self.db_cnx, sql, bindings,
            string_sanitize=2, text_width=9999, use_unicode=True
        )
