from ...job import Job
from .scrap_iexcloud_company_financials import ScrapIEXCloudCompanyFinancials
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_iexcloud_company_financials import DaoIEXCloudCompanyFinancials
from re import split
from pandas import DataFrame

from sqlalchemy import create_engine


class JobIEXCloudCompanyFinancials(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapIEXCloudCompanyFinancials(),
            dao: MariaDbDao = DaoIEXCloudCompanyFinancials(
                create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self, **kwargs):
        sandbox_view = kwargs.get("sandbox_view") if kwargs.get("sandbox_view") else True

        symbols = self.dao.get_symbols(nasdaq100=("=", "1"))
        # symbols = symbols[symbols.symbol=='AEP'].copy()
        # symbols = symbols.iloc[symbols[symbols['symbol']=="CTSH"].index.item()+1:, :].copy()

        for symbol in symbols["symbol"]:
            for dataset in ["income_statement", "balance_sheet", "cash_flow"]:
                print(f"GET {dataset.upper()} FOR {symbol}")
                for period, last in zip(["annual"], [4]):
                    df = self.scrap.run(
                        dataset,
                        symbol=symbol,
                        sandbox_view=sandbox_view,
                        period=period,
                        last=last)
                    df.columns = ['_'.join(split('(?=[A-Z])', x)).lower() for x in df.columns]
                    if not df.empty:
                        self.commit_financials(df, dataset=dataset)

    def commit_financials(self, df: DataFrame, dataset: str):
        df = df.dropna(axis=1)
        df = df.drop(columns=["id", "key"])
        df = df.rename(columns={"subkey": "period"})
        self.dao.commit(f"company_{dataset}", df, replace_values=True)
