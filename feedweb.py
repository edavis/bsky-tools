#!/usr/bin/env python3

from feeds import FeedManager
from feeds.rapidfire import RapidFireFeed
from feeds.popular import PopularFeed
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton')
def get_feed_skeleton():
    manager = FeedManager()
    manager.register(RapidFireFeed)
    # manager.register(PopularFeed)

    try:
        limit = int(request.args.get('limit', 50))
    except ValueError:
        limit = 50

    try:
        offset = int(request.args.get('cursor', 0))
    except ValueError:
        offset = 0

    if request.args['feed'].endswith('-dev'):
        feed_uri = request.args['feed'].replace('-dev', '')
    else:
        feed_uri = request.args['feed']

    langs = request.accept_languages
    posts = manager.serve_feed(feed_uri, limit, offset, langs)
    offset += len(posts)

    return dict(cursor=str(offset), feed=[dict(post=uri) for uri in posts])

if __name__ == '__main__':
    from feedweb_utils import did_doc
    app.add_url_rule('/.well-known/did.json', view_func=did_doc)
    app.run(debug=True)
