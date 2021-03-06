from typing import List

from pandas import DataFrame, read_html

from ...scrap import Scrap
from typing import AnyStr
import logging


class ScrapSp500(Scrap):
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        self.logger = logging.getLogger("ScrapSp500")

    def run(self) -> DataFrame:
        views: List = self.fetch(self.url)
        output: DataFrame = self.parse(views)
        print("SCRAPED: SP500")
        return output

    def parse(self, text, **kwargs) -> DataFrame:
        df = text[0]

        df = df[['Symbol', 'GICS Sector', 'GICS Sub-Industry']].rename(
            columns={'Symbol': 'symbol',
                     'GICS Sector': 'sector',
                     'GICS Sub-Industry': 'sub_sector'})

        return df

    def fetch(self, url: AnyStr) -> List:
        print('HTTP GET request to URL: %s' % url, end='')
        views = read_html(self.url)
        return views


if __name__ == '__main__':
    print('Hi')

