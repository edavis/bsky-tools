#!/usr/bin/env python3

from feeds import Manager
from feeds.rapidfire import RapidFireFeed
from feeds.popular import PopularFeed
from flask import Flask, request, jsonify

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

@app.errorhandler(500)
def internal_server_error(error):
    post = dict(post='at://did:plc:4nsduwlpivpuur4mqkbfvm6a/app.bsky.feed.post/3kp2glnz4r52z')
    obj = {'feed': [post]}
    return jsonify(obj), 500

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton')
def get_feed_skeleton():
    manager = Manager()
    manager.register(RapidFireFeed)
    manager.register(PopularFeed)

    try:
        limit = int(request.args.get('limit', 50))
    except ValueError:
        limit = 50

    try:
        offset = int(request.args.get('cursor', 0))
    except ValueError:
        offset = 0

    feed_uri = request.args['feed']
    langs = request.accept_languages
    posts = manager.serve(feed_uri, limit, offset, langs)
    offset += len(posts)

    return dict(cursor=str(offset), feed=[dict(post=uri) for uri in posts])

if __name__ == '__main__':
    app.run(debug=True)
