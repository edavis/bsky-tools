from atproto import CAR

def commit_ops(payload):
    # TODO(ejd): figure out how to validate blocks
    car_parsed = CAR.from_bytes(payload['blocks'])
    for op in payload['ops']:
        repo_op = op.copy()
        if op['cid'] is not None:
            repo_op['cid'] = op['cid'].encode('base32')
            repo_op['record'] = car_parsed.blocks[repo_op['cid']]
        yield repo_op
