import pandas as pd

class TPL:
    r"""
    Documentation Here
    """

    def __init__(self):

        self.raw_file = None
        self.info = TPLInfo()

    def read_info(self):
        r"""
        Documentation here
        """
        pass

    def read_raw_file(self, filepath:str):
        r"""
        Documentation here
        """
        with open(filepath, 'r') as file:
            
            self.raw_file = file.read()

        return self.raw_file
    

class TPLInfo:
    r"""
    Documentation here
    """

    def __init__(self):

        self.version = None
        self.input_file = None
        self.pvt_file = None
        self.date = None
        self.project = None
        self.title = None
        self.author = None
        self.network = None
        self.geometry = None
        self.branch = None

    def serialize(self):
        r"""
        Documentation here
        """

        return {
            'version': self.version,
            'input_file': self.input_file,
            'pvt_file': self.pvt_file,
            'date': self.date,
            'project': self.project,
            'title': self.title,
            'author': self.author,
            'network': self.network,
            'geometry': self.geometry,
            'branch': self.branch
        }