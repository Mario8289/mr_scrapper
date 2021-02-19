from pandas import DataFrame, read_html

from ...scrap import Scrap
from typing import AnyStr, List
import logging


class ScrapFtse(Scrap):
    def __init__(self):
        self.url = "https://www.fidelity.co.uk/shares"
        self.logger = logging.getLogger("ScrapFtse100")

    def run(self, ftse) -> DataFrame:
        views: List = self.fetch(f"{self.url}/{ftse}")
        output: DataFrame = self.parse(views)
        print("SCRAPED: FTSE100")
        return output

    def parse(self, views: List, **kwargs) -> DataFrame:
        df = views[0][["EPIC", "Sector"]]
        return df

    def fetch(self, url: AnyStr) -> List:
        print('HTTP GET request to URL: %s' % url, end='')
        views = read_html(url)
        return views


if __name__ == '__main__':
    print('Hi')

