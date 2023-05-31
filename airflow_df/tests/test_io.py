import pandas as pd
import os
import unittest

class TestIO(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()
    
    def test_read_csv_as_df(self):
        r"""
        Documentation here
        """
        from ..io import IO
        filepath = os.path.join("data", "Employee Sample Data.csv")
        df = IO.read_csv(filepath_or_buffer=filepath, encoding='unicode_escape')

        self.assertIsInstance(df, pd.DataFrame)