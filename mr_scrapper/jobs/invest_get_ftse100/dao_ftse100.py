from ...dao.mariadb import MariaDbDao

import pandas as pd


class DaoFtse100(MariaDbDao):
    def __init__(self, engine):
        super().__init__(engine)

    def _with_connection(self, query):
        with self.engine.connect() as con:
            result = con.execute(query)
        return result

    def merge_instrument_with_asset_by_name(self, instrument):
        assets = pd.read_sql("SELECT * FROM invest.financial_asset", con=self.engine)
        instruments_with_asset = instrument.merge(assets[['asset_id', 'name']])
        return instruments_with_asset

    def get_product_id(self, broker, product) -> int:
        broker_id = self._with_connection(f"SELECT broker_id from invest.broker WHERE name = '{broker}';").fetchone()[0]
        product_id = self._with_connection(f"SELECT product_id from invest.product WHERE name = '{product}' "
                                           f"and broker_id = {broker_id};").fetchone()[0]
        return product_id
