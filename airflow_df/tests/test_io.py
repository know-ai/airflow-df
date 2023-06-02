import pandas as pd
import os
import unittest
from ..io import IO
from airflow.decorators.base import _TaskDecorator as TaskDecorator

class TestIO(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()
    
    def test_read_csv(self):
        
        with self.subTest(f"Has read_csv method?"):

            self.assertTrue(hasattr(IO, 'read_csv'))

        with self.subTest(f"Is instance of TaskDecorator?"):

            self.assertIsInstance(IO.read_csv, TaskDecorator)

        with self.subTest(f"Read One File"):

            filepath = os.path.join("data", "csv", "Employee Sample Data.csv")
            task = IO.read_csv(filepath, encoding='unicode_escape')
            task = task.operator.python_callable
            df = task(filepath, encoding='unicode_escape')
            self.assertIsInstance(df, pd.DataFrame)

        with self.subTest(f"Read one file with no extension provided"):

            filepath = os.path.join("data", "csv", "Employee Sample Data")
            task = IO.read_csv(filepath, encoding='unicode_escape')
            task = task.operator.python_callable
            df = task(filepath, encoding='unicode_escape')
            self.assertIsInstance(df, pd.DataFrame)

        with self.subTest(f"Read all files in a directory"):

            filepath = os.path.join("data", "csv")
            task = IO.read_csv(filepath, encoding='unicode_escape')
            task = task.operator.python_callable
            df = task(filepath, encoding='unicode_escape')
            self.assertIsInstance(df, pd.DataFrame)

    def test_read_tpl(self):

        with self.subTest(f"Has read_tpl method?"):

            self.assertTrue(hasattr(IO, 'read_tpl'))

        with self.subTest(f"Is instance of TaskDecorator?"):

            self.assertIsInstance(IO.read_tpl, TaskDecorator)

        with self.subTest(f"Read one tpl file"):

            filepath = os.path.join("data", "tpl", "Example1.tpl")
            task = IO.read_tpl(filepath)
            task = task.operator.python_callable
            df = task(filepath)
            self.assertIsInstance(df, pd.DataFrame)