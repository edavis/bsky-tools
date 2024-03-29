import dag_cbor
import redis
from atproto import CAR
from io import BytesIO

def subscribe_commits(redis_cnx=None, parse_car_blocks=False):
    if redis_cnx is None:
        redis_cnx = redis.Redis()
    redis_sub = redis_cnx.pubsub(ignore_subscribe_messages=True)
    redis_sub.subscribe('bsky-tools:firehose:stream')

    for event in redis_sub.listen():
        frame = BytesIO(event['data'])
        header = dag_cbor.decode(frame, allow_concat=True)
        if header['op'] != 1 or header['t'] != '#commit':
            continue

        payload = dag_cbor.decode(frame)
        if payload['tooBig']:
            # TODO(ejd): figure out how to get blocks out-of-band
            continue

        # TODO(ejd): figure out how to validate blocks
        blocks = payload.pop('blocks')
        if parse_car_blocks:
            car_parsed = CAR.from_bytes(blocks)

        message = payload.copy()
        del message['ops']
        message['commit'] = message['commit'].encode('base32')

        for commit_op in payload['ops']:
            op = commit_op.copy()
            if op['cid'] is not None:
                op['cid'] = op['cid'].encode('base32')
                if parse_car_blocks:
                    op['record'] = car_parsed.blocks[op['cid']]
            yield message, op
