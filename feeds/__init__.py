class Manager:
    def __init__(self):
        self.feeds = []
        self.webs = {}

    def register(self, feed):
        f = feed()
        self.webs[feed.FEED_URI] = f
        self.feeds.append(f)

    def process(self, commit):
        for feed in self.feeds:
            feed.process(commit)

    def serve(self, feed_uri, limit, offset):
        feed = self.webs.get(feed_uri)
        if feed is not None:
            return feed.serve(limit, offset)
