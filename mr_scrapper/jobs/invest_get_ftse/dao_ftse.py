from ...dao.mariadb import MariaDbDao

import pandas as pd


class DaoFtse(MariaDbDao):
    def __init__(self, engine):
        super().__init__(engine)

    def _with_connection(self, query):
        with self.engine.connect() as con:
            result = con.execute(query)
        return result
