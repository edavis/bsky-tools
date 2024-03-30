import dag_cbor
import redis
import sys
import websockets
from atproto import CAR
from io import BytesIO

async def subscribe_commits():
    relay_url = 'ws://127.0.0.1:9060'
    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
        while True:
            frame = BytesIO(await firehose.recv())
            header = dag_cbor.decode(frame, allow_concat=True)
            if header['op'] != 1 or header['t'] != '#commit':
                continue

            payload = dag_cbor.decode(frame)
            if payload['tooBig']:
                # TODO(ejd): figure out how to get blocks out-of-band
                continue

            # TODO(ejd): figure out how to validate blocks
            blocks = payload.pop('blocks')
            car_parsed = CAR.from_bytes(blocks)

            message = payload.copy()
            del message['ops']
            message['commit'] = message['commit'].encode('base32')

            for commit_op in payload['ops']:
                op = commit_op.copy()
                if op['cid'] is not None:
                    op['cid'] = op['cid'].encode('base32')
                    op['record'] = car_parsed.blocks[op['cid']]
                yield message, op

async def bsky_activity():
    redis_cnx = redis.Redis()
    relay_url = 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
    firehose_seq = redis_cnx.get('dev.edavis.muninsky.seq')
    if firehose_seq:
        relay_url += f'?cursor={firehose_seq.decode()}'

    sys.stdout.write(f'opening websocket connection to {relay_url}\n')
    sys.stdout.flush()

    async with websockets.connect(relay_url, ping_timeout=None) as firehose:
        while True:
            frame = BytesIO(await firehose.recv())
            header = dag_cbor.decode(frame, allow_concat=True)
            if header['op'] != 1 or header['t'] != '#commit':
                continue

            payload = dag_cbor.decode(frame)
            if payload['tooBig']:
                # TODO(ejd): figure out how to get blocks out-of-band
                continue

            # TODO(ejd): figure out how to validate blocks
            blocks = payload.pop('blocks')
            car_parsed = CAR.from_bytes(blocks)

            message = payload.copy()
            del message['ops']
            message['commit'] = message['commit'].encode('base32')

            for commit_op in payload['ops']:
                op = commit_op.copy()
                if op['cid'] is not None:
                    op['cid'] = op['cid'].encode('base32')
                    op['record'] = car_parsed.blocks[op['cid']]
                yield message, op
