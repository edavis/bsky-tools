from datetime import datetime, timezone, timedelta

class BaseFeed:
    def process_commit(self, commit):
        raise NotImplementedError

    def serve_feed(self, limit, offset, langs):
        raise NotImplementedError

    def run_tasks_minute(self):
        pass

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
        parsed = self.parse_timestamp(timestamp)
        utc_now = datetime.now(timezone.utc)
        if parsed.timestamp() <= 0:
            return utc_now
        elif parsed - timedelta(minutes=2) < utc_now:
            return parsed
        elif parsed > utc_now:
            return utc_now

class FeedManager:
    def __init__(self):
        self.feeds = {}

    def register(self, feed):
        self.feeds[feed.FEED_URI] = feed()

    def process_commit(self, commit):
        for feed in self.feeds.values():
            feed.process_commit(commit)

    def serve_feed(self, feed_uri, limit, offset, langs):
        feed = self.feeds.get(feed_uri)
        if feed is not None:
            return feed.serve_feed(limit, offset, langs)

    def serve_feed_debug(self, feed_uri, limit, offset, langs):
        feed = self.feeds.get(feed_uri)
        if feed is not None:
            return feed.serve_feed_debug(limit, offset, langs)

    def run_tasks_minute(self):
        for feed in self.feeds.values():
            feed.run_tasks_minute()
