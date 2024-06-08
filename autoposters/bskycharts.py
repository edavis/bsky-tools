#!/usr/bin/env python3

import time

import atproto
import requests


BSKY_HANDLE = 'bskycharts.edavis.dev'
BSKY_APP_PASSWORD = ''
BSKY_ACTIVITY_IMAGE_URL = 'https://bskycharts.edavis.dev/munin-cgi/munin-cgi-graph/edavis.dev/bskycharts.edavis.dev/bsky-day.png'


def main():
    client = atproto.Client()
    client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)

    resp = requests.get(BSKY_ACTIVITY_IMAGE_URL)
    resp.raise_for_status()

    client.send_image(
        text = '',
        image = resp.content,
        image_alt = 'munin chart showing daily bluesky network activity'
    )


if __name__ == '__main__':
    main()
