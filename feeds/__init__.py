class Manager:
    def __init__(self):
        self.feeds = {}

    def register(self, feed):
        self.feeds[feed.FEED_URI] = feed()

    def process(self, commit):
        for _, feed in self.feeds.items():
            feed.process(commit)

    def serve(self, feed_uri, limit, offset):
        feed = self.feeds.get(feed_uri)
        if feed is not None:
            return feed.serve(limit, offset)
