import os
import glob
from . import TPL
from . import Genkey
from collections import namedtuple
from os.path import dirname, basename, splitext
from types import GeneratorType

class Olga:
    """
    Reads .tpl and .genkey files.
    """

    def __init__(self):
        
        self.__file = namedtuple("File", "tpl genkey")
        self.__extensions = ['.tpl', '.genkey']

    def group_olga_files(self, tpl: TPL, genkey: Genkey)->namedtuple:
        """
        Groups Olga files (.tpl and .genkey) into an structure
    
        **Parameters**

        - **tpl:** (TPL) A TPL Object
        - **genkey:** (Genkey) An Genkey Object

        **Returns**

        - **object:** (namedtuple) With object.tpl / object.genkey
        """
        if isinstance(tpl, TPL) & isinstance(genkey, Genkey):

            file = self.__file(tpl, genkey)
            return file

    @staticmethod
    def get_files(filepath: str, ext:str=".tpl") -> list:
        """
        Gets all the files contained in a folder. Returns a list of files.

        **Parameters**

        - **filepath:** (str) Path to the folder.
        - **ext:** (str) 

        **Returns**

        - **filenames:** (list) List of filenames with "ext" inside "filepath"
        """
        if not ext.startswith("."):
            ext = f".{ext}"

        filepath = filepath.split(os.sep)
        filepath.append(f"*{ext}")
        filepath = os.sep.join(filepath)

        return glob.glob(filepath)
    
    def remove_file_extension(self, filename:str)->str:
        """
        Check if the filename has an extension section valid (.tpl or .genkey), 
        if extension is not provided, return the same filename

        **Parameters**

        - **filename:** (str) Filename with or without extension file

        **Returns**

        - **filename:** (str) filename without extension, so another method read "filename.tpl and filename.genkey" file
        """
        folder_name, filename = dirname(filename), basename(filename)
        filename, ext = splitext(filename)

        if ext:

            if ext not in self.__extensions:

                raise NameError(f"{ext} file is not allowed - Only use {self.__extensions}")

        return os.path.join(folder_name, filename)
    
    def read_tpl(self, filename:str)->TPL:
        """
        Reads .tpl olga file given its filename

        **Parameters**

        - **filename:** (str) Filename with or without its entension file.

        **Returns**

        - **object:** (Genkey) Genkey object
        """
        filename = self.remove_file_extension(filename)
        tpl = TPL()
        tpl.read(filename + '.tpl')
        return tpl
    
    def read_genkey(self, filename:str)->Genkey:
        """
        Reads .genkey olga file given its filename

        **Parameters**

        - **filename:** (str) Filename with or without its entension file.

        **Returns**

        - **object:** (Genkey) Genkey object
        """
        filename = self.remove_file_extension(filename)
        genkey = Genkey()
        genkey.read(filename + '.genkey')
        return genkey
    
    def read_folder(self, path:str):
        """
        Reads all olga file located in a path

        **Parameters**

        - **path:** (str) path where Ola files are.
        
        **Returns**

        - **object:** (GeneratorType) with 'tpl' and 'genkey' attributes
        """
        files = self.get_files(filepath=path)
            
        for file in files:

            yield self.read_file(file)

    def read_file(self, filename:str)->GeneratorType:
        """
        Reads one group of files, only a .tpl and .genkey
    
        **Parameters**

        - **filename:** (str) Filename with or without its entension file.

        **Returns**

        - **object:** (GeneratorType) with 'tpl' and 'genkey' attributes
        """
        filename = self.remove_file_extension(filename)
        tpl = self.read_tpl(filename)
        genkey = self.read_genkey(filename)
        return self.group_olga_files(tpl, genkey)

    def read(self, filepath)->GeneratorType:
        """
        Reads olga files (.tpl and .genkey) into a Olga Object Structure

        **Parameters**

        - **filepath:** (str) filename with/without olga exntesion (.tpl - .genkey) or foldername where are all olga files

        **Returns**

        - **object:** (GeneratorType) with 'tpl' and 'genkey' attributes

        ```python
        from airflow_df.IO import Olga
        import os

        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath)
        ```
        """
        
        if os.path.isdir(filepath):

            files = self.read_folder(filepath)
            for file in files:

                yield file

        else:

            yield self.read_file(filepath)