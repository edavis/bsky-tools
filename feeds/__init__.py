class FeedManager:
    def __init__(self):
        self.feeds = {}

    def register(self, feed):
        self.feeds[feed.FEED_URI] = feed()

    def process(self, commit):
        for feed in self.feeds.values():
            feed.process(commit)

    def serve(self, feed_uri, limit, offset, langs):
        feed = self.feeds.get(feed_uri)
        if feed is not None:
            return feed.serve(limit, offset, langs)
