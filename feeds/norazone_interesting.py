import logging

from atproto import Client, models
import apsw
import apsw.ext

from . import BaseFeed

# https://bsky.app/profile/nora.zone/post/3kv35hqi4a22b
TARGET_QUOTE_URI = 'at://did:plc:4qqizocrnriintskkh6trnzv/app.bsky.feed.post/3kv35hqi4a22b'

class NoraZoneInteresting(BaseFeed):
    FEED_URI = 'at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.generator/nz-interesting'

    def __init__(self):
        self.client = Client('https://public.api.bsky.app')

    def process_commit(self, commit):
        pass

    def commit_changes(self):
        pass

    def serve_feed(self, limit, cursor, langs):
        quotes = self.client.app.bsky.feed.get_quotes(
            models.AppBskyFeedGetQuotes.Params(
                uri = TARGET_QUOTE_URI,
                limit = limit,
                cursor = cursor,
            )
        )
        return {
            'cursor': quotes.cursor,
            'feed': [dict(post=post.uri) for post in quotes.posts],
        }
