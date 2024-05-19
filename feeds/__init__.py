from datetime import datetime, timezone, timedelta

class BaseFeed:
    def process_commit(self, commit):
        raise NotImplementedError

    def serve_feed(self, limit, offset, langs):
        raise NotImplementedError

    def serve_wildcard_feed(self, feed_uri, limit, offset, langs):
        raise NotImplementedError

    def commit_changes(self):
        raise NotImplementedError

    def parse_timestamp(self, timestamp):
        # https://atproto.com/specs/lexicon#datetime
        formats = {
            # preferred
            '1985-04-12T23:20:50.123Z': '%Y-%m-%dT%H:%M:%S.%f%z',
            # '1985-04-12T23:20:50.123456Z': '%Y-%m-%dT%H:%M:%S.%f%z',
            # '1985-04-12T23:20:50.120Z': '%Y-%m-%dT%H:%M:%S.%f%z',
            # '1985-04-12T23:20:50.120000Z': '%Y-%m-%dT%H:%M:%S.%f%z',

            # supported
            # '1985-04-12T23:20:50.12345678912345Z': '',
            '1985-04-12T23:20:50Z': '%Y-%m-%dT%H:%M:%S%z',
            # '1985-04-12T23:20:50.0Z': '%Y-%m-%dT%H:%M:%S.%f%z',
            # '1985-04-12T23:20:50.123+00:00': '%Y-%m-%dT%H:%M:%S.%f%z',
            # '1985-04-12T23:20:50.123-07:00': '%Y-%m-%dT%H:%M:%S.%f%z',
        }

        for format in formats.values():
            try:
                ts = datetime.strptime(timestamp, format)
            except ValueError:
                continue
            else:
                return ts

        return datetime.now(timezone.utc)

    def safe_timestamp(self, timestamp):
        utc_now = datetime.now(timezone.utc)
        if timestamp is None:
            return utc_now

        parsed = self.parse_timestamp(timestamp)
        if parsed.timestamp() <= 0:
            return utc_now
        elif parsed - timedelta(minutes=2) < utc_now:
            return parsed
        elif parsed > utc_now:
            return utc_now

    def transaction_begin(self, db):
        if not db.in_transaction:
            db.execute('BEGIN')

    def transaction_commit(self, db):
        if db.in_transaction:
            db.execute('COMMIT')

    def wal_checkpoint(self, db, mode='PASSIVE'):
        db.pragma(f'wal_checkpoint({mode})')
