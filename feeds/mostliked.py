import logging

import apsw
import apsw.ext
from expiringdict import ExpiringDict
import threading
import queue

from . import BaseFeed

# store post in database once it has this many likes
MIN_LIKES = 5

class DatabaseWorker(threading.Thread):
    def __init__(self, name, db_path, task_queue):
        super().__init__()
        self.db_cnx = apsw.Connection(db_path)
        self.db_cnx.pragma('foreign_keys', True)
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')
        self.stop_signal = False
        self.task_queue = task_queue
        self.logger = logging.getLogger(f'feeds.db.{name}')
        self.changes = 0

    def run(self):
        while True:
            task = self.task_queue.get(block=True)
            if task == 'STOP':
                self.logger.debug('received STOP, breaking now')
                break
            elif task == 'COMMIT':
                self.logger.debug(f'committing {self.changes} changes')
                if self.db_cnx.in_transaction:
                    self.db_cnx.execute('COMMIT')
                    checkpoint = self.db_cnx.execute('PRAGMA wal_checkpoint(PASSIVE)')
                    self.logger.debug(f'checkpoint: {checkpoint.fetchall()!r}')
                    self.changes = 0
                self.logger.debug(f'qsize: {self.task_queue.qsize()}')
            else:
                sql, bindings = task
                if not self.db_cnx.in_transaction:
                    self.db_cnx.execute('BEGIN')
                self.db_cnx.execute(sql, bindings)
                self.changes += self.db_cnx.changes()
            self.task_queue.task_done()

        self.logger.debug('closing database connection')
        self.db_cnx.close()

    def stop(self):
        self.task_queue.put('STOP')

class MostLikedFeed(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/most-liked'
    DELETE_OLD_POSTS_QUERY = """
    delete from posts where create_ts < unixepoch('now', '-23 hours');
    """

    def __init__(self):
        self.db_cnx = apsw.Connection('db/mostliked.db')
        self.db_cnx.pragma('foreign_keys', True)
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (
              uri text primary key,
              create_ts timestamp,
              likes int
            );
            create table if not exists langs (
              uri text,
              lang text,
              foreign key(uri) references posts(uri) on delete cascade
            );
            create index if not exists ts_idx on posts(create_ts);
            """)

        self.logger = logging.getLogger('feeds.mostliked')
        self.drafts = ExpiringDict(max_len=50_000, max_age_seconds=5*60)

        self.db_writes = queue.Queue()
        self.db_worker = DatabaseWorker('mostliked', 'db/mostliked.db', self.db_writes)
        self.db_worker.start()

    def stop_db_worker(self):
        self.logger.debug('sending STOP')
        self.db_writes.put('STOP')

    def process_commit(self, commit):
        if commit['opType'] != 'c':
            return

        if commit['collection'] == 'app.bsky.feed.post':
            record = commit.get('record')
            post_uri = f"at://{commit['did']}/app.bsky.feed.post/{commit['rkey']}"

            # to keep the DB in check, instead of adding every post right away
            # we make note of it but only add to DB once it gets some likes
            self.drafts[post_uri] = {
                'ts': self.safe_timestamp(record.get('createdAt')).timestamp(),
                'langs': record.get('langs', []),
                'likes': 0,
            }

        elif commit['collection'] == 'app.bsky.feed.like':
            record = commit.get('record')
            try:
                subject_uri = record['subject']['uri']
            except KeyError:
                return

            if subject_uri in self.drafts:
                record_info = self.drafts.pop(subject_uri).copy()
                record_info['likes'] += 1
                if record_info['likes'] < MIN_LIKES:
                    self.drafts[subject_uri] = record_info
                    return

                self.logger.debug(f'graduating {subject_uri}')

                task = (
                    'insert or ignore into posts (uri, create_ts, likes) values (:uri, :ts, :likes)',
                    {'uri': subject_uri, 'ts': record_info['ts'], 'likes': record_info['likes']}
                )
                self.db_writes.put(task)

                for lang in record_info['langs']:
                    task = (
                        'insert or ignore into langs (uri, lang) values (:uri, :lang)',
                        {'uri': subject_uri, 'lang': lang}
                    )
                    self.db_writes.put(task)

            subject_exists = self.db_cnx.execute('select 1 from posts where uri = ?', [subject_uri])
            if subject_exists.fetchone() is None:
                return

            task = (
                'update posts set likes = likes + 1 where uri = :uri',
                {'uri': subject_uri}
            )
            self.db_writes.put(task)

    def commit_changes(self):
        self.db_writes.put((self.DELETE_OLD_POSTS_QUERY, {}))
        self.db_writes.put('COMMIT')
        self.logger.debug(f'there are {len(self.drafts)} drafts')

    def generate_sql(self, limit, offset, langs):
        bindings = []
        sql = """
        select posts.uri, create_ts, create_ts - unixepoch('now', '-24 hours') as ttl, likes, lang
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
        order by likes desc, create_ts desc
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
