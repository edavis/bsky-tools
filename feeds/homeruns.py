import logging

import apsw
import apsw.ext

from . import BaseFeed

MLBHRS_DID = 'did:plc:pnksqegntq5t3o7pusp2idx3'

TEAM_ABBR_LOOKUP = {
    "OAK":"OaklandAthletics",
    "PIT":"PittsburghPirates",
    "SDN":"SanDiegoPadres",
    "SEA":"SeattleMariners",
    "SFN":"SanFranciscoGiants",
    "SLN":"StLouisCardinals",
    "TBA":"TampaBayRays",
    "TEX":"TexasRangers",
    "TOR":"TorontoBlueJays",
    "MIN":"MinnesotaTwins",
    "PHI":"PhiladelphiaPhillies",
    "ATL":"AtlantaBraves",
    "CHA":"ChicagoWhiteSox",
    "MIA":"MiamiMarlins",
    "NYA":"NewYorkYankees",
    "MIL":"MilwaukeeBrewers",
    "LAA":"LosAngelesAngels",
    "ARI":"ArizonaDiamondbacks",
    "BAL":"BaltimoreOrioles",
    "BOS":"BostonRedSox",
    "CHN":"ChicagoCubs",
    "CIN":"CincinnatiReds",
    "CLE":"ClevelandGuardians",
    "COL":"ColoradoRockies",
    "DET":"DetroitTigers",
    "HOU":"HoustonAstros",
    "KCA":"KansasCityRoyals",
    "LAN":"LosAngelesDodgers",
    "WAS":"WashingtonNationals",
    "NYN":"NewYorkMets",
}

class HomeRunsTeamFeed(BaseFeed):
    FEED_URI = 'at://did:plc:pnksqegntq5t3o7pusp2idx3/app.bsky.feed.generator/team:*'

    def __init__(self):
        self.db_cnx = apsw.Connection('db/homeruns.db')
        self.db_cnx.pragma('journal_mode', 'WAL')
        self.db_cnx.pragma('wal_autocheckpoint', '0')

        with self.db_cnx:
            self.db_cnx.execute("""
            create table if not exists posts (uri text, tag text);
            create index if not exists tag_idx on posts(tag);
            """)

        self.logger = logging.getLogger('feeds.homeruns')

    def process_commit(self, commit):
        if commit['repo'] != MLBHRS_DID:
            return

        op = commit['op']
        if op['action'] != 'create':
            return

        collection, _ = op['path'].split('/')
        if collection != 'app.bsky.feed.post':
            return

        record = op.get('record')
        if record is None:
            return

        uri = 'at://{repo}/{path}'.format(
            repo = commit['repo'],
            path = op['path']
        )
        tags = record.get('tags', [])

        self.logger.debug(f'adding {uri!r} under {tags!r}')

        with self.db_cnx:
            for tag in tags:
                self.db_cnx.execute(
                    "insert into posts (uri, tag) values (:uri, :tag)",
                    dict(uri=uri, tag=tag)
                )

    def commit_changes(self):
        self.logger.debug('committing changes')
        self.wal_checkpoint(self.db_cnx, 'RESTART')

    def serve_wildcard_feed(self, feed_uri, limit, offset, langs):
        prefix, sep, team_abbr = feed_uri.rpartition(':')
        team_tag = TEAM_ABBR_LOOKUP[team_abbr]

        cur = self.db_cnx.execute("""
        select uri
        from posts
        where tag = :tag
        order by uri desc
        limit :limit offset :offset
        """, dict(tag=team_tag, limit=limit, offset=offset))

        return [uri for (uri,) in cur]

    def serve_wildcard_feed_debug(self, feed_uri, limit, offset, langs):
        pass
