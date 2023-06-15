import os
import pandas as pd

from . import TPL


class OlgaFormatter(list):
    r"""
    Documentation here
    """

    def __init__(self):
        self.__olga = None

    @staticmethod
    def read(filepath) -> pd.DataFrame | list:
        r"""
        Documentation here
        """
        if os.path.isfile(filepath) & filepath.endswith('tpl'):
            tpl = TPL()
            tpl.read(filepath=filepath)

        pass
