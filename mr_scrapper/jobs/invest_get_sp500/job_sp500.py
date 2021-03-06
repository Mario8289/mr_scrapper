from ...job import Job
from .scrap_sp500 import ScrapSp500
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_sp500 import DaoSp500

from sqlalchemy import create_engine


class JobSp500(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapSp500(),
            dao: MariaDbDao = DaoSp500(create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self):
        df = self.scrap.run()
        self.commit_symbols(df)

    def commit_symbols(self, df):
        df['sp500'] = 1
        df_with_exchange = self.dao.get_exchange_for_symbol_by_region(df, region="US")
        self.dao.commit("symbol", df_with_exchange, replace_values=True)