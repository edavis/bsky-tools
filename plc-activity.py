#!/usr/bin/env python3

from datetime import datetime, timezone
import sys
import json
import redis
import requests
import time

PLC_EXPORT_URL = 'https://plc.directory/export'

cids = []
redis_conn = redis.Redis()
redis_pipe = redis_conn.pipeline()

while True:
    plc_latest = redis_conn.get('dev.edavis.muninsky.plc_latest')
    assert plc_latest is not None, 'manually set the `plc_latest` redis key first'
    ts = datetime.fromisoformat(plc_latest.decode())
    ts = ts.isoformat('T', 'milliseconds').replace('+00:00', 'Z')

    qs = '?after={ts}'.format(ts=ts)

    print(f'Requesting {PLC_EXPORT_URL}{qs}', end=' ')
    resp = requests.get(PLC_EXPORT_URL + qs)
    resp.raise_for_status()

    ops = 0
    after = datetime.now(timezone.utc)
    for line in resp.iter_lines():
        doc = json.loads(line)
        after = datetime.strptime(doc['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        if doc['cid'] in cids:
            continue
        cids.insert(0, doc['cid'])
        redis_pipe.incr('dev.edavis.muninsky.plc_ops')
        ops += 1

    print(f'Fetched {ops} operations')
    sys.stdout.flush()
    cids = cids[:25]

    redis_pipe.set('dev.edavis.muninsky.plc_latest', after.isoformat())
    redis_pipe.execute()

    time.sleep(5)
