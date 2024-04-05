from feeds.popular import PopularFeed
from feeds.rapidfire import RapidFireFeed

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

    def commit_changes(self):
        for feed in self.feeds.values():
            feed.commit_changes()

feed_manager = FeedManager()
feed_manager.register(PopularFeed)
feed_manager.register(RapidFireFeed)
