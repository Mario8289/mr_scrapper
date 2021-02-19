from pandas import DataFrame
from typing import AnyStr
import logging

from yahooquery import Ticker

from ...scrap import Scrap


class ScrapYahooFinanceCompanyFinancials(Scrap):
    def __init__(self):
        self.logger = logging.getLogger("ScrapIEXCloudReferenceData")

    def run(self, dataset: AnyStr, symbol: AnyStr, **kwargs) -> DataFrame:
        stock = Ticker(symbol)
        source_df = self.fetch(dataset, stock, **kwargs)
        if not source_df.empty:
            parsed_df = self.parse(source_df, dataset=dataset, symbol=symbol)
            return parsed_df
        else:
            return source_df

    def parse(self, data, **kwargs) -> DataFrame:
        dataset = kwargs.get("dataset")
        symbol = kwargs.get("symbol")
        if dataset == "income_statement":
            return self.parse_income_statement(data, symbol)
        elif dataset == "cash_flow":
            return self.parse_cash_flow(data, symbol)
        elif dataset == "balance_sheet":
            return self.parse_balance_sheet(data, symbol)

    def fetch(self, dataset: AnyStr, ticker: Ticker, **kwargs):
        if dataset == "income_statement":
            output = ticker.income_statement(frequency=kwargs.get("period"), trailing=False)
        elif dataset == "cash_flow":
            output = ticker.cash_flow(frequency=kwargs.get("period"), trailing=False)
        elif dataset == "balance_sheet":
            output = ticker.balance_sheet(frequency=kwargs.get("period"), trailing=False)
        else:
            return DataFrame()

        if type(output) == DataFrame:
            return output
        else:
            self.logger.warning(output)
            return DataFrame()

    @staticmethod
    def _map_period(v):
        if v == '12M':
            return 'annual'
        elif v == '3M':
            return 'quarterly'
        else:
            return None

    def parse_balance_sheet(self, df: DataFrame, symbol: AnyStr) -> DataFrame:
        column_mapping = {
            "asOfDate": "fiscal_date",
            "periodType": "period",
            "AccountsPayable":  "accounts_payable",
            "CommonStock":  "common_stock",
            "TreasuryStock": "treasury_stock",
            "CurrentAssets":  "current_assets",
            "CashCashEquivalentsAndShortTermInvestments":  "current_cash",
            "CurrentDebt":  "current_long_term_debt",
            "Goodwill":  "goodwill",
            "Inventory":  "inventory",
            "LongTermDebt":  "long_term_debt",
            "TotalNonCurrentAssets":  "long_term_investments",
            "NetTangibleAssets":  "net_tangible_assets",
            "OtherNonCurrentAssets":  "other_assets",
            "OtherCurrentAssets":  "other_current_assets",
            "NetPPE":  "property_plant_equipment",
            "Receivables":  "receivables",
            "RetainedEarnings":  "retained_earnings",
            "CommonStockEquity":  "shareholder_equity",
            "TotalAssets":  "total_assets",
            "CurrentLiabilities":  "total_current_liabilities",
            "TotalLiabilitiesNetMinorityInterest":  "total_liabilities",
        }
        df_subset = df[[x for x in list(column_mapping.keys()) if x in df.columns]].copy()
        df_subset.columns = [column_mapping[x] for x in df_subset.columns]

        df_subset = self.parse_common_columns(df_subset, symbol)

        return df_subset

    def parse_income_statement(self, df: DataFrame, symbol: AnyStr) -> DataFrame:

        column_mapping = {
            "asOfDate": "fiscal_date",
            "periodType": "period",
            "CostOfRevenue": "cost_of_revenue",
            "OperatingIncome": "ebit",
            "GrossProfit": "gross_profit",
            "TaxProvision": "income_tax",
            "InterestExpense": "interest_income",
            "TaxEffectOfUnusualItems": "minority_interest",
            "NetIncome": "net_income",
            "TotalExpenses": "operating_expense",
            "OperatingIncome": "operating_income",
            "TaxEffectOfUnusualItems": "other_income_expense_net",
            "PretaxIncome": "pretax_income",
            "ResearchAndDevelopment": "research_and_development",
            "SellingGeneralAndAdministration": "selling_general_and_admin",
            "TotalRevenue": "total_revenue"
        }
        df_subset = df[[x for x in list(column_mapping.keys()) if x in df.columns]].copy()
        df_subset.columns = [column_mapping[x] for x in df_subset.columns]

        df_subset = self.parse_common_columns(df_subset, symbol)

        return df_subset

    def parse_cash_flow(self, df: DataFrame, symbol: AnyStr) -> DataFrame:

        column_mapping = {
            "asOfDate": "fiscal_date",
            "periodType": "period",
            "NetIncome": "net_income",
            "CashDividendsPaid": "dividends_paid",
            "OperatingCashflow": "cash_flow",
            "InvestingCashflow": "total_investing_cash_flows",
            "DepreciationAndAmortization": "depreciation",
            "PurchaseOfInvestment": "investments",
            "PurchaseOfBusiness": "purchase_of_business",
            "RepaymentOfDebt": "repayment_of_debt",
            "ChangeInInventory": "changes_in_inventories",
            "ChangesInAccountReceivables": "changes_in_receivables",
            "CapitalExpenditure": "capital_expenditures"

        }
        cash_change = None
        if all([x in df.columns for x in ['EndCashPosition', 'BeginningCashPosition']]):
            cash_change = df['EndCashPosition'] - df['BeginningCashPosition']

        df_subset = df[[x for x in list(column_mapping.keys()) if x in df.columns]].copy()
        df_subset.columns = [column_mapping[x] for x in df_subset.columns]
        df_subset['cash_change'] = cash_change
        df_subset = self.parse_common_columns(df_subset, symbol)

        return df_subset

    def parse_common_columns(self, df: DataFrame, symbol: AnyStr) -> DataFrame:
        df['fiscal_year'] = df['fiscal_date'].dt.year
        df['fiscal_quarter'] = df['fiscal_date'].dt.quarter
        df['fiscal_date'] = df['fiscal_date'].map(lambda x: x.strftime("%Y-%m-%d"))
        df['source'] = 'yahoo'
        df['currency'] = 'USD'
        df['symbol'] = symbol
        df['period'] = df['period'].apply(self._map_period)
        return df


if __name__ == '__main__':
    print('Hi')