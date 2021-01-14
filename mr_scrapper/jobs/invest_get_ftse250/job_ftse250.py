from ...job import Job
from .scrap_ftse250 import ScrapFtse250
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_ftse250 import DaoSp500

from sqlalchemy import create_engine


class JobFtse250(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapFtse250(),
            dao: MariaDbDao = DaoSp500(create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self):
        df = self.scrap.run()

        self.commit_financial_assets(df)

        self.commit_instruments(df)

        # TODO: no need to update any instrument that are no longer in the FTSE100

    def commit_instruments(self, df):
        products = [("trading212", "invest")]
        for (broker, product) in products:
            product_id = self.dao.get_product_id(broker, product)
            df_instrument = df.copy()
            df_instrument = df_instrument.drop(columns=["symbol"])
            df_instrument['currency'] = "GBP"
            df_instrument['price_increment'] = 0.01
            df_instrument['product_id'] = product_id
            df_instrument_with_asset = self.dao.merge_instrument_with_asset_by_name(df_instrument)
            self.dao.commit("instrument", df_instrument_with_asset, replace_values=True)

    def commit_financial_assets(self, df):
        df_asset = df.copy()
        df_asset['currency'] = "GBP"
        df_asset['ftse250'] = 1
        df_asset['asset_class'] = "STOCK"
        df_asset['hidden'] = 0
        self.dao.commit("financial_asset", df_asset, replace_values=True)