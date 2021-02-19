from pandas import DataFrame

from ...scrap import Scrap
from typing import AnyStr
import logging

from iexcloud.reference import Reference


class ScrapIEXCloudReferenceData(Scrap):
    def __init__(self):
        self.logger = logging.getLogger("ScrapIEXCloudReferenceData")

    def run(self, dataset: AnyStr, **kwargs) -> DataFrame:
        reference = Reference(sandbox_view=False)
        source_df = self.fetch(dataset, reference, **kwargs)
        parsed_df = self.parse(dataset, source_df)
        return parsed_df

    def parse(self, dataset, source_df) -> DataFrame:
        if dataset == "international_symbols":
            return source_df[["name", "type", "symbol", "exchange", "region", "currency", "isEnabled"]]
        elif dataset == "international_exchanges":
            return source_df
        elif dataset == "us_symbols":
            return source_df[["name", "type", "symbol", "exchange", "region", "currency", "isEnabled"]]
        elif dataset == "us_exchanges":
            return source_df[["longName", "refId", "mic"]]

    def fetch(self, dataset: AnyStr, reference: Reference, **kwargs):
        if dataset == "international_symbols":
            return reference.get_international_symbols(kwargs.get("exchange"))
        elif dataset == "international_exchanges":
            return reference.get_international_exchanges()
        elif dataset == "us_symbols":
            return reference.get_us_symbols()
        elif dataset == "us_exchanges":
            return reference.get_us_exchanges()


if __name__ == '__main__':
    print('Hi')

