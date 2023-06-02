from .templates import Templates

class DAG:
    r"""
    Documentation here
    """

    @staticmethod
    def create(name:str, file:str):
        r"""
        Documentation here
        """
        with open(f'{name}.py', 'w') as f:

            f.write(Templates.lineal(name, file))

# if __name__=="__main__":

#     DAG.create(name="demo", file="dag_tag_value.csv")