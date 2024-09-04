#!/usr/bin/env python3

from flask import Flask, request, jsonify
from prometheus_client import Counter, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.datastructures import LanguageAccept

from feed_manager import feed_manager

feed_requests = Counter('feed_requests', 'requests by feed URI', ['feed'])

app = Flask(__name__)

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton')
def get_feed_skeleton():
    try:
        limit = int(request.args.get('limit', 50))
    except ValueError:
        limit = 50

    try:
        offset = int(request.args.get('cursor', 0))
    except ValueError:
        offset = 0

    feed_uri = request.args['feed']
    if feed_uri.endswith('-dev'):
        feed_uri = feed_uri.replace('-dev', '')
    else:
        (prefix, sep, rkey) = feed_uri.rpartition('/')
        feed_requests.labels(rkey).inc()

    if request.args.getlist('langs'):
        req_langs = request.args.getlist('langs')
        langs = LanguageAccept([(l, 1) for l in req_langs])
    else:
        langs = request.accept_languages

    if request.args.get('debug', '0') == '1':
        headers = {'Content-Type': 'text/plain; charset=utf-8'}
        debug = feed_manager.serve_feed(feed_uri, limit, offset, langs, debug=True)
        return debug, headers

    posts = feed_manager.serve_feed(feed_uri, limit, offset, langs, debug=False)

    if len(posts) == limit:
        offset += len(posts)
        return dict(cursor=str(offset), feed=[dict(post=uri) for uri in posts])
    else:
        return dict(feed=[dict(post=uri) for uri in posts])

app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})

if __name__ == '__main__':
    from feedweb_utils import did_doc
    app.add_url_rule('/.well-known/did.json', view_func=did_doc)

    app.run(debug=True)
