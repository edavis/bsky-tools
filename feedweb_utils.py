NGROK_HOSTNAME = 'routinely-right-barnacle.ngrok-free.app'

def did_doc():
    return {
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': f'did:web:{NGROK_HOSTNAME}',
        'service': [
            {
                'id': '#bsky_fg',
                'type': 'BskyFeedGenerator',
                'serviceEndpoint': f'https://{NGROK_HOSTNAME}',
            },
        ],
    }
