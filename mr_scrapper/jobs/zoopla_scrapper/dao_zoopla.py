from ...dao.google_drive import GoogleDriveDao


class DaoZoopla(GoogleDriveDao):
    def __init__(self, credentials="mycreds.txt"):
        super().__init__(credentials)

    def get_post_code_district(self):
        folder_id = self.get_folder_id(folder="scraped")
        file = self.get_file_id(folder_id=folder_id, title="postcode_districts.csv")[0]
        return self.load_file(file)