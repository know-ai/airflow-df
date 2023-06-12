from unittest import TestLoader, TestSuite, TextTestRunner
# from airflow_df.tests.test_io import TestIO
from airflow_df.tests.test_tpl import TestTPL

def suite():
    r"""
    Documentation here
    """
    tests = list()
    suite = TestSuite()
    # tests.append(TestLoader().loadTestsFromTestCase(TestIO))
    tests.append(TestLoader().loadTestsFromTestCase(TestTPL))

    suite = TestSuite(tests)
    return suite


if __name__=='__main__':
    
    runner = TextTestRunner()
    runner.run(suite())