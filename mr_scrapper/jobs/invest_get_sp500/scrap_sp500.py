from pandas import DataFrame, read_html

from ...scrap import Scrap
from typing import AnyStr
import logging


class ScrapSp500(Scrap):
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        self.logger = logging.getLogger("ScrapSp500")

    def run(self) -> DataFrame:
        html = self.fetch(self.url)
        output = self.parse(html)
        print("SCRAPED: SP500")
        return output

    def parse(self, text, **kwargs) -> DataFrame:
        df = text[0]
        for col in ['Security', 'GICS Sector', 'GICS Sub-Industry']:
            df[col] = df[col].map(lambda x: x.lower())

        df = df[['Security', 'Symbol', 'GICS Sector', 'GICS Sub-Industry']].rename(
            columns={'Security': 'name',
                     'Symbol': 'symbol',
                     'GICS Sector': 'sector',
                     'GICS Sub-Industry': 'sub_sector'})

        return df

    def fetch(self, url: AnyStr):
        print('HTTP GET request to URL: %s' % url, end='')
        html = read_html(self.url)
        return html


if __name__ == '__main__':
    print('Hi')

