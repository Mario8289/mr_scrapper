from mr_scrapper.dao.google_drive import GoogleDrive

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
# gauth.LocalWebserverAuth() # client_secrets.json need to be in the same directory as the script
gauth.SaveCredentials("mycreds.txt")
gauth.LoadCredentialsFile("mycreds.txt")  # SaveCredentialsFile

drive = GoogleDrive(gauth)
id = '1DlWYB8U3tqBik5lehAcEVn-3MgTfoYd9'
zoopla_id = '1DlWYB8U3tqBik5lehAcEVn-3MgTfoYd9'
folder = drive.CreateFile({'title': 'tt101',
                           'parents': [{"kind": "drive#fileLink", "id": zoopla_id}],
                           "mimeType": "application/vnd.google-apps.folder"})
folder.Upload()

folder = drive.ListFile({'q': "title = 'to_rent' and  '1DlWYB8U3tqBik5lehAcEVn-3MgTfoYd9' in parents and trashed=false"}).GetList()[0] # get the folder we just created
file = drive.CreateFile({'title': "what22.csv", 'parents': [{'id': folder['id']}]})
file.SetContentString('Hello, World, you, fool\nt, f, s')
file.Upload()

# replace the value of this variable
# with the absolute path of the directory
# import os
# path = os.getcwd()
# # iterating thought all the files/folder
# # of the desired directory
# for x in os.listdir(path):
#     f = drive.CreateFile({'title': x})
#     f.SetContentFile(os.path.join(path, x))
#     f.Upload()
#
#     # Due to a known bug in pydrive if we
#     # don't empty the variable used to
#     # upload the files to Google Drive the
#     # file stays open in memory and causes a
#     # memory leak, therefore preventing its
#     # deletion
#     f = None

# View all folders and file in your Google Drive
folderList = drive.ListFile({'q': "'1DlWYB8U3tqBik5lehAcEVn-3MgTfoYd9' in parents and trashed=false and mimeType = 'application/vnd.google-apps.folder'"}).GetList()
fileList = drive.ListFile({'q': "'1DlWYB8U3tqBik5lehAcEVn-3MgTfoYd9' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'"}).GetList()


for file in folderList:
  print('Title: %s, ID: %s' % (file['title'], file['id']))
  # Get the folder ID that you want
  if(file['title'] == "scraped"):
      fileID = file['id']

  if (file['title'] == "Quals"):
      fileID2 = file['id']

file1 = drive.CreateFile({"mimeType": "text/csv", "parents": [{"kind": "drive#fileLink", "id": fileID}]})
file2 = drive.CreateFile({"mimeType": "text/csv", "parents": [{"kind": "drive#fileLink", "id": fileID2}]})
file1.SetContentFile("small_file.csv")
file1.Upload() # Upload the file.
print('Created file %s with mimeType %s' % (file1['title'], file1['mimeType']))