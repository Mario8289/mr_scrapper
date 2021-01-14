from pandas import DataFrame, read_html

from ...scrap import Scrap
from typing import AnyStr
import logging


class ScrapFtse250(Scrap):
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/FTSE_250_Index"
        self.logger = logging.getLogger("ScrapFtse250")

    def run(self) -> DataFrame:
        html = self.fetch(self.url)
        output = self.parse(html)
        print("SCRAPED: FTSE250")
        return output

    def parse(self, text, **kwargs) -> DataFrame:
        df = text[1]

        df.columns = ['name', 'symbol']
        df["name"] = df["name"].map(lambda x: x.lower())

        return df

    def fetch(self, url: AnyStr):
        print('HTTP GET request to URL: %s' % url, end='')
        html = read_html(self.url)
        return html


if __name__ == '__main__':
    print('Hi')

