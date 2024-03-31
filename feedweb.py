#!/usr/bin/env python3

from feeds import Manager
from feeds.rapidfire import RapidFireFeed

from flask import Flask, request
app = Flask(__name__)

@app.route('/.well-known/did.json')
def well_known_did():
    service = {
        'id': '#bsky_fg',
        'type': 'BskyFeedGenerator',
        'serviceEndpoint': 'https://feedgen.edavis.dev',
    }
    return {
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': 'did:web:feedgen.edavis.dev',
        'service': [service],
    }

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton')
def get_feed_skeleton():
    manager = Manager()
    manager.register(RapidFireFeed)

    try:
        limit = int(request.args.get('limit', 50))
    except ValueError:
        limit = 50

    try:
        offset = int(request.args.get('cursor', 0))
    except ValueError:
        offset = 0

    feed_uri = request.args['feed']
    return manager.serve(feed_uri, limit, offset)


if __name__ == '__main__':
    app.run(debug=True)
