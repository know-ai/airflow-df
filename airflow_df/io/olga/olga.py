import os
import glob
from . import TPL
from . import Genkey
from collections import namedtuple
from os.path import dirname, basename, splitext

class Olga:
    """Reads and saves the tpl files into a list of TPL objects.
    """

    def __init__(self):
        self.__file = namedtuple("File", "tpl genkey")
        self.__extensions = ['.tpl', '.genkey']

    def append(self, tpl: TPL, genkey: Genkey):
        """Documentation here
        """
        if isinstance(tpl, TPL) & isinstance(genkey, Genkey):

            file = self.__file(tpl, genkey)
            return file
            # super(Olga, self).append(file)

    @staticmethod
    def get_files(filepath: str, ext:str=".tpl") -> list:
        """Gets all the files contained in a folder. Returns a list of files.

    **Parameters**

        - **filepath:** (str) Path to the folder.
        - **ext:** (str) 
        """
        if not ext.startswith("."):
            ext = f".{ext}"

        filepath = filepath.split(os.sep)
        filepath.append(f"*{ext}")
        filepath = os.sep.join(filepath)

        return glob.glob(filepath)
    
    def __check_filename(self, path:str):
        """Documentation here
        """
        folder_name, filename = dirname(path), basename(path)
        filename, ext = splitext(filename)

        if ext:

            if ext not in self.__extensions:

                raise NameError(f"{ext} file is not allowed - Only use {self.__extensions}")

        return os.path.join(folder_name, filename)
    
    def __read_tpl(self, filename:str):
        """Documentation here
        """
        tpl = TPL()
        tpl.read(filename + '.tpl')
        return tpl
    
    def __read_genkey(self, filename:str):
        """Documentation here
        """
        genkey = Genkey()
        genkey.read(filename + '.genkey')
        return genkey
    
    def __read_folder(self, path:str):
        """Documentation here
        """
        files = self.get_files(filepath=path)
            
        for file in files:

            yield self.__read_file(file)

    def __read_file(self, filename:str):
        """Documentation here
        """
        filename = self.__check_filename(filename)
        tpl = self.__read_tpl(filename)
        genkey = self.__read_genkey(filename)
        return self.append(tpl, genkey)

    def read(self, filepath):
        """Read olga files into a Olga Object Structure

    **Parameters**

-       **filepath:** (str) file location
        """
        
        if os.path.isdir(filepath):

            return self.__read_folder(filepath)

        else:

            yield self.__read_file(filepath)

