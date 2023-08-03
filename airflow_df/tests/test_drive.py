import os
import unittest
from pydrive2.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive


class TestDrive(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()
    
    def test_read_csv(self):

        gauth = GoogleAuth()

        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json')
        drive = GoogleDrive(gauth)