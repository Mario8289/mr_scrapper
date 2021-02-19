from ...dao.mariadb import MariaDbDao

import pandas as pd


class DaoNasdaq100(MariaDbDao):
    def __init__(self, engine):
        super().__init__(engine)

    def _with_connection(self, query):
        with self.engine.connect() as con:
            result = con.execute(query)
        return result

    def get_exchange_for_symbol_by_region(self, symbols: pd.DataFrame, region: str):
        all_symbols = pd.read_sql(f"SELECT symbol, exchange, region FROM invest.symbol where REGION = '{region}'",
                                  con=self.engine)
        symbols_with_exchange = symbols.merge(all_symbols)
        return symbols_with_exchange
