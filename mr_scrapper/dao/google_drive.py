from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from typing import List
from io import StringIO

import pandas as pd


class GoogleDriveDao:

    def __init__(self, credentials="mycreds.txt"):
        self.drive = self._connect(credentials)

    @staticmethod
    def _connect(credentials):
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(credentials)
        if gauth.credentials is None:
            # Authenticate if they're not there
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            # Refresh them if expired
            gauth.Refresh()
        else:
            # Initialize the saved creds
            gauth.Authorize()
        drive = GoogleDrive(gauth)
        return drive

    def reconnect(self, crendentials):
        self.drive = self._connect(crendentials)

    def get_folder_id(self, folder: str, parents: str = None, create: bool = False) -> List:
        base = "trashed=false and mimeType = 'application/vnd.google-apps.folder'"
        if not parents:
            q = f"'root' in parents and title = '{folder}' and {base}"
            folder_id = self.get_id(q)
            if not folder_id and create:
                self.create_folder(folder, parent=None)
                folder_id = self.get_id(q)
        else:
            folder_id = ['root']
            for folder in parents.split('/') + [folder]:
                q = f"'{folder_id[0]}' in parents and title = '{folder}' and {base}"
                folder_id_iter = self.get_id(q)
                if not folder_id_iter and create:
                    self.create_folder(folder, parent=folder_id)
                    folder_id_iter = self.get_id(q)
                folder_id = folder_id_iter
        return folder_id

    def get_file_id(self, folder_id: str = None, title: str = None) -> List:
        q = " trashed=false and mimeType != 'application/vnd.google-apps.folder'"
        ids: List = []
        if title:
            q = f"title = '{title}' and {q}"
        if folder_id:
            for folder in folder_id:
                q = f"'{folder}' in parents and {q}"
                ids.extend(self.get_id(q))
        return ids

    def create_folder(self, folder, parent=None):
        if parent:
            folder = self.drive.CreateFile(
                {'title': folder,
                 'parents': [{"kind": "drive#fileLink", "id": parent}],
                 "mimeType": "application/vnd.google-apps.folder"}
            )
        else:
            folder = self.drive.CreateFile(
                {'title': folder,
                 "mimeType": "application/vnd.google-apps.folder"}
            )

        folder.Upload()

    def get_id(self, q: str) -> List:
        files = self.drive.ListFile({'q': q}).GetList()
        if len(files) != 0:
            return [x['id'] for x in files]
        else:
            return []

    def upload_string(self, string: str, file: str, folder_id: str = None):
        if folder_id:
            file = self.drive.CreateFile({'title': file, 'parents': [{'id': folder_id}]})
        else:
            file = self.drive.CreateFile({'title': file})

        file.SetContentString(string)
        file.Upload()

    def upload_file(self, string: str, file: str, folder_id: str = None):
        if folder_id:
            file = self.drive.CreateFile({'title': file, 'parents': [{'id': folder_id}]})
        else:
            file = self.drive.CreateFile({'title': file})

        file.SetContentFile(string)
        file.Upload()

    def delete_file(self, file_id: str, permanently=True):
        file = self.drive.CreateFile({'id': file_id})
        if permanently:
            file.Delete()
        else:
            file.Trash()

    def load_file(self, file_id: str, sep: str = ','):
        file_obj = self.drive.CreateFile({'id': file_id})
        data = StringIO(file_obj.GetContentString())
        return pd.read_csv(data, sep=sep)


if __name__ == "__main__":
    g = GoogleDriveDao()
    print('test over')