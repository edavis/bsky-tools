from fnmatch import fnmatchcase

from feeds.battle import BattleFeed
from feeds.rapidfire import RapidFireFeed
from feeds.popular import PopularFeed
from feeds.homeruns import HomeRunsTeamFeed
from feeds.norazone_interesting import NoraZoneInteresting
from feeds.sevendirtywords import SevenDirtyWordsFeed
from feeds.ratio import RatioFeed
from feeds.mostliked import MostLikedFeed
from feeds.outlinetags import OutlineTagsFeed
from feeds.popqp import PopularQuotePostsFeed

class FeedManager:
    def __init__(self):
        self.feeds = {}

    def register(self, feed):
        self.feeds[feed.FEED_URI] = feed()

    def process_commit(self, commit):
        for feed in self.feeds.values():
            feed.process_commit(commit)

    def serve_feed(self, feed_uri, limit, offset, langs, debug=False):
        for pattern, feed in self.feeds.items():
            if fnmatchcase(feed_uri, pattern):
                break
        else:
            raise Exception('no matching feed pattern found')

        if '*' in pattern and debug:
            return feed.serve_wildcard_feed_debug(feed_uri, limit, offset, langs)

        elif '*' in pattern and not debug:
            return feed.serve_wildcard_feed(feed_uri, limit, offset, langs)

        elif '*' not in pattern and debug:
            return feed.serve_feed_debug(limit, offset, langs)

        elif '*' not in pattern and not debug:
            return feed.serve_feed(limit, offset, langs)

    def commit_changes(self):
        for feed in self.feeds.values():
            feed.commit_changes()

feed_manager = FeedManager()
# feed_manager.register(RapidFireFeed)
feed_manager.register(PopularFeed)
feed_manager.register(HomeRunsTeamFeed)
feed_manager.register(NoraZoneInteresting)
feed_manager.register(SevenDirtyWordsFeed)
feed_manager.register(MostLikedFeed)
feed_manager.register(PopularQuotePostsFeed)
