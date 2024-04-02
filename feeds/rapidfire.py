import sqlite3

MAX_TEXT_LENGTH = 140

class RapidFireFeed:
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/rapidfire'

    def __init__(self):
        self.db_cnx = sqlite3.connect('/dev/shm/feedgens/rapidfire.db')
        with self.db_cnx:
            self.db_cnx.executescript(
                "pragma journal_mode = WAL;"
                "pragma synchronous = OFF;"
                "create table if not exists posts (uri text, create_ts timestamp, lang text);"
                "create index if not exists create_ts_idx on posts(create_ts);"
            )

    def process(self, commit):
        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.post':
            return

        ts = commit['time']
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

            with self.db_cnx:
                langs = record.get('langs') or ['']
                for lang in langs:
                    self.db_cnx.execute(
                        'insert into posts (uri, create_ts, lang) values (:uri, :ts, :lang)',
                        dict(uri=post_uri, ts=ts, lang=lang)
                    )

                self.db_cnx.execute(
                    "delete from posts where strftime('%s', create_ts) < strftime('%s', 'now', '-15 minutes')"
                )

    def serve(self, limit, offset, langs):
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
