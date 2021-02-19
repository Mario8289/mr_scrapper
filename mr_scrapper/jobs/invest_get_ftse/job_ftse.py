from ...job import Job
from .scrap_ftse import ScrapFtse
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_ftse import DaoFtse

from sqlalchemy import create_engine


class JobFtse(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapFtse(),
            dao: MariaDbDao = DaoFtse(create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self):

        df_ftse100 = self.scrap.run("ftse-100")
        self.commit_symbols(df_ftse100, 100)

        df_ftse200 = self.scrap.run("ftse-250")
        self.commit_symbols(df_ftse200, 250)

        df_ftse350 = self.scrap.run("ftse-350")
        self.commit_symbols(df_ftse350, 350)

    def commit_symbols(self, df, ftse_indice: int):
        df["exchange"] = "LON"
        df[f"ftse{ftse_indice}"] = 1
        df = df.rename(columns={"EPIC": "symbol", "Sector": "sector"})
        self.dao.commit("symbol", df, replace_values=True)