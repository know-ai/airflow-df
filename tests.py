from unittest import TestLoader, TestSuite, TextTestRunner
from airflow_df.tests.test_io import TestIO
from airflow_df.tests.test_tpl import TestTPL
from airflow_df.tests.test_olga import TestOlga
from airflow_df.tests.test_genkey import TestGenkey
# from airflow_df.tests.test_drive import TestDrive

def suite():
    r"""
    Documentation here
    """
    tests = list()
    suite = TestSuite()
    tests.append(TestLoader().loadTestsFromTestCase(TestIO))
    tests.append(TestLoader().loadTestsFromTestCase(TestTPL))
    tests.append(TestLoader().loadTestsFromTestCase(TestGenkey))
    tests.append(TestLoader().loadTestsFromTestCase(TestOlga))
    # tests.append(TestLoader().loadTestsFromTestCase(TestDrive))

    suite = TestSuite(tests)
    return suite


if __name__=='__main__':
    
    runner = TextTestRunner()
    runner.run(suite())