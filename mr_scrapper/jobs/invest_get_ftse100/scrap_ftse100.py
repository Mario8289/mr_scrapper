from pandas import DataFrame

from ...scrap import Scrap
from typing import AnyStr, List
import logging

import requests
from bs4 import BeautifulSoup


class ScrapFtse100(Scrap):
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/FTSE_100_Index"
        self.logger = logging.getLogger("ScrapFtse100")

    def run(self) -> DataFrame:
        html = self.fetch(self.url)
        output = self.parse(html.text)
        print("SCRAPED: FTSE100")
        return output

    def parse(self, text, **kwargs) -> DataFrame:

        soup = BeautifulSoup(text, 'lxml')

        table = soup.findAll("table", attrs={"class": "wikitable"})[2]
        table_date = table.tbody.find_all("tr")

        company: List = []
        symbol: List = []
        sector: List = []

        for row in table_date[1:]:
            company.append(row.contents[1].text.lower())
            symbol.append(row.contents[3].text)
            sector.append(row.contents[5].text.replace("\n", "").lower())

        df = DataFrame(columns=['name', 'symbol', 'sector'], data=list(zip(company, symbol, sector)))
        return df

    def fetch(self, url: AnyStr):
        print('HTTP GET request to URL: %s' % url, end='')
        html = requests.get(url)
        return html


if __name__ == '__main__':
    print('Hi')

