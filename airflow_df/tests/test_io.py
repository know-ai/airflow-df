import pandas as pd
import os
import unittest
from ..io import IO

class TestIO(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()
    
    def test_read_csv(self):
        
        with self.subTest(f"Has read_csv method?"):

            self.assertTrue(hasattr(IO, 'read_csv'))

        with self.subTest(f"Read one file"):

            filepath = os.path.join("data", "csv", "Employee Sample Data.csv")
            df = IO.read_csv(filepath, encoding='unicode_escape')
            self.assertIsInstance(df, pd.DataFrame)

        # with self.subTest(f"Read one file with no extension provided"):

        #     filepath = os.path.join("data", "csv", "Employee Sample Data")
        #     df = IO.read_csv(filepath, encoding='unicode_escape')
        #     self.assertIsInstance(df, pd.DataFrame)

        # with self.subTest(f"Read all files in a directory"):

        #     filepath = os.path.join("data", "csv")
        #     df = IO.read_csv(filepath, encoding='unicode_escape')
        #     self.assertIsInstance(df, pd.DataFrame)

    # def test_read_tpl(self):

    #     with self.subTest(f"Has read_tpl method?"):

    #         self.assertTrue(hasattr(IO, 'read_tpl'))

    #     with self.subTest(f"Read one tpl file"):

    #         filepath = os.path.join("data", "tpl", "Example1.tpl")
    #         df = IO.read_tpl(filepath)
    #         self.assertIsInstance(df, pd.DataFrame)