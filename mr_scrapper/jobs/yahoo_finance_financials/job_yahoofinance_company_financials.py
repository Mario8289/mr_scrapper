from ...job import Job
from .scrap_yahoofinance_company_financials import ScrapYahooFinanceCompanyFinancials
from ...scrap import Scrap
from ...dao.mariadb import MariaDbDao
from .dao_yahoofinance_company_financials import DaoYahooFinanceCompanyFinancials
from re import split
from pandas import DataFrame

from sqlalchemy import create_engine


class JobYahooFinanceCompanyFinancials(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapYahooFinanceCompanyFinancials(),
            dao: MariaDbDao = DaoYahooFinanceCompanyFinancials(
                create_engine("mysql+pymysql://test:password@12@localhost:3384/invest"))):
        self.scrap = scrap
        self.dao = dao

    def run(self, **kwargs):
        symbols = self.dao.get_symbols(ftse100=("=", "1"))
        symbols = symbols[symbols.symbol == 'BP.'].copy()
        # symbols = symbols.iloc[symbols[symbols['symbol'] == "TCOM"].index.item() + 1:, :].copy()

        for idx, row in symbols.iterrows():
            symbol = row['symbol'].replace('.', '')
            suffix = row['yahoo_suffix']
            if suffix:
                symbol = f"{symbol}{suffix}"
            for dataset in ["income_statement", "balance_sheet", "cash_flow"]:
                print(f"GET {dataset.upper()} FOR {symbol}")
                for period in ["q", "a"]:
                    df = self.scrap.run(
                        dataset,
                        symbol,
                        period=period)
                    df.columns = ['_'.join(split('(?=[A-Z])', x)).lower() for x in df.columns]
                    if not df.empty:
                        self.commit_financials(df, dataset=dataset)

    def commit_financials(self, df: DataFrame, dataset: str):
        df = df.dropna(axis=1)
        self.dao.commit(f"company_{dataset}", df, replace_values=True)
