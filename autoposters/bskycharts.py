#!/usr/bin/env python3

import time

import atproto


BSKY_HANDLE = 'bskycharts.edavis.dev'
BSKY_APP_PASSWORD = ''
BSKY_ACTIVITY_IMAGE_PATH = '/var/www/munin/edavis.dev/bskycharts.edavis.dev/bsky-day.png'


def main():
    time.sleep(10) # let the charts finish updating

    client = atproto.Client()
    client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)

    with open(BSKY_ACTIVITY_IMAGE_PATH, 'rb') as chart:
        client.send_image(
            text = '',
            image = chart.read(),
            image_alt = 'munin chart showing daily bluesky network activity'
        )


if __name__ == '__main__':
    main()
