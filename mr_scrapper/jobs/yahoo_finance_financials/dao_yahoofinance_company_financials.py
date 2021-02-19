from ...dao.mariadb import MariaDbDao

import pandas as pd


class DaoYahooFinanceCompanyFinancials(MariaDbDao):
    def __init__(self, engine):
        super().__init__(engine)

    def _with_connection(self, query):
        with self.engine.connect() as con:
            result = con.execute(query)
        return result

    def get_symbols(self, **kwargs):
        query_string = [f"{k} {v[0]} {v[1]}" for (k, v) in kwargs.items()]
        symbols = pd.read_sql("SELECT * FROM invest.symbol JOIN exchange using (exchange) "
                              f"WHERE {' and '.join(query_string)}", con=self.engine)
        return symbols
