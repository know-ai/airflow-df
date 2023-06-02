class Templates:
    r"""
    Documentation here
    """

    @classmethod
    def lineal(cls, name:str, file:str, config:dict={}):
        r"""
        Documentation here
        """
        return f"""from airflow import DAG
import pandas as pd

filepath = "{file}"

with DAG(dag_id="{name}") as dag:

    df = pd.read_csv(filepath)
    pd.keys(df)"""