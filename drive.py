from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def authenticate_with_google_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Opens a browser window for authentication
    drive = GoogleDrive(gauth)
    return drive
