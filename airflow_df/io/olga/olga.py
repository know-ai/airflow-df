import os
import glob
import pandas as pd

from . import TPL


class OlgaFormatter(list):
    r"""
    Documentation here
    """

    def append(self, tpl: TPL):
        """Add tpl to the OlgaFormatter list.

    **Parameters**

        **tpl:** (TPL) tpl object to be appended.
        """
        if tpl:
            super().append(tpl)

    @staticmethod
    def read_file(filepath: str) -> TPL:
        """Reads one tpl file. Retunrs a TPL object.

    **Parameters**

        **filepath:** (str) Path to the file.
        """
        if os.path.isfile(filepath) & filepath.endswith('.tpl'):
            tpl = TPL()
            tpl.read(filepath=filepath)

            return tpl

    @staticmethod
    def get_files(filepath: str) -> list:
        """Gets all the files contained in a folder. Returns a list of files.

    **Parameters**

        **filepath:** (str) Path to the folder.
        """
        filepath = filepath.split(os.sep)
        filepath.append("*")
        filepath = os.sep.join(filepath)

        return glob.glob(filepath)

    def read(self, filepath) -> TPL | list:
        """Read .tpl file into a TPL Object Structure

**Parameters**

-       **filepath:** (str) .tpl file location
        """
        if os.path.isdir(filepath):
            files = self.get_files(filepath=filepath)

            for file in files:
                tpl = self.read_file(filepath=file)

                self.append(tpl)

            return self

        tpl = self.read_file(filepath=filepath)
        self.append(tpl)

        return tpl
