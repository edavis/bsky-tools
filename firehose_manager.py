import logging

import apsw

class FirehoseManager:
    def __init__(self, fname='firehose.db'):
        self.db_cnx = apsw.Connection(fname)
        self.db_cnx.pragma('journal_mode', 'WAL')
        with self.db_cnx:
            self.db_cnx.execute("create table if not exists firehose(key text unique, value text)")

        self.logger = logging.getLogger('feeds.firehose')

    def get_sequence_number(self):
        cur = self.db_cnx.execute("select * from firehose where key = 'seq'")
        row = cur.fetchone()
        if row is None:
            return None
        (key, value) = row
        return int(value)

    def set_sequence_number(self, value):
        self.logger.debug(f'setting sequence number = {value}')

        with self.db_cnx:
            self.db_cnx.execute(
                "insert into firehose (key, value) values ('seq', :value) on conflict(key) do update set value = :value",
                dict(value=value)
            )

        self.db_cnx.pragma('wal_checkpoint(TRUNCATE)')
