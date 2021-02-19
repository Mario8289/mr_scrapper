from pandas import DataFrame

from ...scrap import Scrap
from typing import AnyStr
import logging

from iexcloud.stock import Stock


class ScrapIEXCloudCompanyFinancials(Scrap):
    def __init__(self):
        self.logger = logging.getLogger("ScrapIEXCloudReferenceData")

    def run(self, dataset: AnyStr, **kwargs) -> DataFrame:
        stock = Stock(sandbox_view=False)
        source_df = self.fetch(dataset, stock, **kwargs)
        parsed_df = self.parse(dataset, source_df)
        return parsed_df

    def parse(self, dataset, source_df) -> DataFrame:
        source_df['source'] = 'iex_cloud'

        if dataset == "income_statement":
            return source_df
        elif dataset == "cash_flow":
            return source_df
        elif dataset == "balance_sheet":
            return source_df

    def fetch(self, dataset: AnyStr, reference: Stock, **kwargs):
        if dataset == "income_statement":
            return reference.get_income_statement(kwargs.get("symbol"), kwargs.get("period"), kwargs.get("last"))
        elif dataset == "cash_flow":
            return reference.get_cashflow(kwargs.get("symbol"), kwargs.get("period"), kwargs.get("last"))
        elif dataset == "balance_sheet":
            return reference.get_balance_sheet(kwargs.get("symbol"), kwargs.get("period"), kwargs.get("last"))


if __name__ == '__main__':
    print('Hi')

