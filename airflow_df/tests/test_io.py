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
            generator_iterable = task(filepath, encoding='unicode_escape')
            for item in generator_iterable:
                
                self.assertIsInstance(item.csv.data.df, pd.DataFrame)

        with self.subTest(f"Read one file with no extension provided"):

            filepath = os.path.join("data", "csv", "Employee Sample Data")
            task = IO.read_csv(filepath, encoding='unicode_escape')
            task = task.operator.python_callable
            generator_iterable = task(filepath, encoding='unicode_escape')
            for item in generator_iterable:
                
                self.assertIsInstance(item.csv.data.df, pd.DataFrame)

        with self.subTest(f"Read all files in a directory"):

            filepath = os.path.join("data", "csv")
            task = IO.read_csv(filepath, encoding='unicode_escape')
            task = task.operator.python_callable
            generator_iterable = task(filepath, encoding='unicode_escape')
            for item in generator_iterable:
                
                self.assertIsInstance(item.csv.data.df, pd.DataFrame)

    def test_read_olga(self):

        with self.subTest(f"Has read_olga method?"):

            self.assertTrue(hasattr(IO, 'read_olga'))

        with self.subTest(f"Is instance of TaskDecorator?"):

            self.assertIsInstance(IO.read_olga, TaskDecorator)

        with self.subTest(f"Read one olga file"):

            filepath = os.path.join("data", "olga", "1")
            task = IO.read_olga(filepath)
            task = task.operator.python_callable
            generator_iterable = task(filepath)
            for item in generator_iterable:

                with self.subTest(f"Generator Item for Olga file hasattr tpl"):
                    
                    self.assertTrue(hasattr(item, 'tpl'))
                
                with self.subTest(f"Generator Item for Olga file hasattr genkey"):
                    
                    self.assertTrue(hasattr(item, 'genkey'))
                