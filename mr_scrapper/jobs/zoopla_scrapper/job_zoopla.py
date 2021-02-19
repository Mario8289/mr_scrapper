from ...job import Job
from .scrap_zoopla import ScrapZoopla
from ...scrap import Scrap
from ...dao.google_drive import GoogleDriveDao
from .dao_zoopla import DaoZoopla
from pandas import DataFrame
import datetime as dt

from pydrive.settings import InvalidConfigError


class JobZoopla(Job):
    def __init__(
            self,
            scrap: Scrap = ScrapZoopla(),
            dao: GoogleDriveDao = DaoZoopla()):
        self.scrap = scrap
        self.dao = dao
        self.listing_types = ['to-rent', 'for-sale']

    def run(self, **kwargs):

        last_post_code: str = ""
        for i in range(5):
            try:
                # setup inputs to ran against
                post_code_districts = self.dao.get_post_code_district()
                post_codes_subset = self.filter_post_code_districts(
                    post_code_districts,
                    postcode=kwargs.get("postcode"),
                    region=kwargs.get("region")
                )
                if last_post_code:
                    post_codes_subset = post_codes_subset.iloc[
                                        post_codes_subset[post_codes_subset.Postcode == last_post_code].index[0]:, ]
                else:
                    post_codes_subset = post_codes_subset.iloc[post_codes_subset[post_codes_subset.Postcode == 'W14'].index[0]:, ]

                listing_types = kwargs.get('listing_types') if kwargs.get('listing_types') else self.listing_types

                for i, row in post_codes_subset.iterrows():
                    for listing_type in listing_types:
                        date_str = str(dt.datetime.now().date())

                        filename = f"{date_str}-zoopla-{listing_type}-{row.Postcode}.csv"

                        folder_id = self.dao.get_folder_id(folder=date_str, parents=f"zoopla/{listing_type}", create=True)
                        file_id = self.dao.get_file_id(folder_id=folder_id, title=filename)
                        if not file_id or kwargs.get('replace'):
                            output = self.scrap.run(post_code_district=row.Postcode, listing_type=listing_type)
                            if output:
                                self.dao.upload_string(output, filename, folder_id)

                                print(f'postcode: {row.Postcode}, listing type: {listing_type}, Uploaded to Google Drive folder {folder_id}')
                            else:
                                print(f'postcode: {row.Postcode}, listing type: {listing_type}, No properties found')

                            if kwargs.get('replace'):
                                for file in file_id:
                                    self.dao.delete_file(file)
                        else:
                            print(f"postcode: {row.Postcode}, listing type: {listing_type}, File Exists in Google Drive folder {folder_id}")
                    last_post_code = row.Postcode
                break

            except InvalidConfigError as e:
                print(e)
                self.dao.reconnect("mycreds.txt")

            except AttributeError as e:
                print(e)
        else:
            raise Exception("ran out of attempts, program has not finished/")

    @staticmethod
    def filter_post_code_districts(df, postcode = None, region=None):
        if region is None and postcode is None:
            return df.copy()
        else:
            if region:
                df = df[df['Region'] == region.capitalize()].copy()
            if postcode:
                df = df[df['Postcode'] == postcode.upper()].copy()
            return df

    def commit_financials(self, df: DataFrame, dataset: str):
        df = df.dropna(axis=1)
        self.dao.commit(f"company_{dataset}", df, replace_values=True)
