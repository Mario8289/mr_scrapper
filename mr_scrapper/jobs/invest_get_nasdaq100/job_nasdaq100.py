from ...job import Job
from .scrap_nasdaq100 import ScrapNasdaq100
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_nasdaq100 import DaoNasdaq100

from sqlalchemy import create_engine


class JobNasdaq100(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapNasdaq100(),
            dao: MariaDbDao = DaoNasdaq100(create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self):
        df = self.scrap.run()
        self.commit_symbols(df)

    def commit_symbols(self, df):
        df['nasdaq100'] = 1
        df_with_exchange = self.dao.get_exchange_for_symbol_by_region(df, region="US")
        self.dao.commit("symbol", df_with_exchange, replace_values=True)