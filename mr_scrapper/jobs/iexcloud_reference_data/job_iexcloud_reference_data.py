from ...job import Job
from .scrap_iexcloud_reference_data import ScrapIEXCloudReferenceData
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_iexcloud_reference_data import DaoIEXCloudReferenceData

from sqlalchemy import create_engine


class JobIEXCloudReferenceData(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapIEXCloudReferenceData(),
            dao: MariaDbDao = DaoIEXCloudReferenceData(
                create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self, **kwargs):
        sandbox_view = kwargs.get("sandbox_view") if kwargs.get("sandbox_view") else True

        # part 1 write the international symbols
        df_int_symbols = self.scrap.run("international_symbols", sandbox_view=sandbox_view, exchange="LON")
        self.commit_international_symbols(df_int_symbols)

        df_int_symbols = self.scrap.run("international_symbols", sandbox_view=sandbox_view, exchange="ETR")

        # part 2 write the international exchanges
        df_int_ex = self.scrap.run("international_exchanges", sandbox_view=sandbox_view)
        self.commit_international_exchanges(df_int_ex)

        # part 3 write the us symbols
        df_us_symbols = self.scrap.run("us_symbols", sandbox_view=sandbox_view)
        self.commit_us_symbols(df_us_symbols)

        # part 4 write the us exchanges
        df_us_ex = self.scrap.run("us_exchanges", sandbox_view=sandbox_view)
        self.commit_us_exchanges(df_us_ex)

    def commit_international_symbols(self, df):
        df['symbol'] = df['symbol'].map(lambda x: x.split("-")[0])
        df['hidden'] = df['isEnabled'].map(lambda x: 0 if x is True else 1)
        df['type'] = df['type'].fillna("uk")
        df = df.drop(columns=["isEnabled"])
        self.dao.commit("symbol", df, replace_values=True)

    def commit_us_symbols(self, df):
        df['hidden'] = df['isEnabled'].map(lambda x: 0 if x is True else 1)
        df = df.drop(columns=["isEnabled"])
        self.dao.commit("symbol", df, replace_values=True)

    def commit_international_exchanges(self, df):
        df = df.rename(columns={"description": "name", "exchangeSuffix": "iex_suffix"})
        df['iex_suffix'] = df['iex_suffix'].map(lambda x: x if x != "-null" else None)
        df['yahoo_suffix'] = df['exchange'].map(lambda x: ".L" if x == 'LON' else None)
        self.dao.commit("exchange", df, replace_values=True)

    def commit_us_exchanges(self, df):
        df = df.rename(columns={"longName": "name", "refId": "exchange"})
        df["region"] = "US"
        df = df[["exchange", "region", "name", "mic"]]
        self.dao.commit("exchange", df, replace_values=True)