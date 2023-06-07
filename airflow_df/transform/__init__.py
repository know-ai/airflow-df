from ..helpers import Helpers
import pandas as pd


@Helpers.as_airflow_tasks()
class Transform:
    r"""
    Documentation here

    """
    @Helpers.check_airflow_task_args
    @staticmethod
    def rename_columns(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.rename(**kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def keep_columns(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.drop(**kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def resample(df:pd.DataFrame, rule, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.resample(rule, **kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def reset_index(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.reset_index(**kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def keep_columns(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.drop(**kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def convert_to_float(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        pass

    @Helpers.check_airflow_task_args
    @staticmethod
    def set_datetime_index(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        pass

    @Helpers.check_airflow_task_args
    @staticmethod
    def info(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """
        print(df.info(**kwargs))
        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def add_columns(df:pd.DataFrame, **kwargs)->pd.DataFrame:
        r"""
        Documentation here
        """

        pass