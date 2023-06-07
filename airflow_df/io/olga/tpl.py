import pandas as pd

class TPL:
    r"""
    Documentation Here
    """

    def __init__(self):

        self.raw_file = None
        self.info = Info()

    def set_info(self):
        r"""
        Documentation here
        """
        pass

    def set_profile(self):
        r"""
        Documentation here
        """
        pass

    def set_columns(self):
        r"""
        Documentation here
        """
        pass

    def set_content(self):
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
    

class Info:
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

    def set_info(self, file:str):
        r"""
        Documentation here
        """
        pass

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
    

class Profile:
    r"""
    Documentation here
    """

    def __init__(self):

        self._x = None
        self._y = None

    def set_profile(self, file:str):
        r"""
        Documentation here
        """
        pass

    @property
    def x(self, values:list):
        r"""
        Documentation here
        """

        self._x = values

    @x.setter
    def x(self):

        return self._x
    
    @property
    def y(self, values:list):
        r"""
        Documentation here
        """

        self._y = values

    @x.setter
    def y(self):

        return self._y
    
    def serialize(self):
        r"""
        Documentation here
        """

        return (self.x, self.y)
    

class Columns:
    r"""
    Documentation here
    """

    def __init__(self):

        self.columns = list()

    def set_columns(self, file:str):
        r"""
        Documentation here
        """

        pass

    def serialize(self):
        r"""
        Documentation here
        """
        pass


class Content:
    r"""
    Documentation here
    """

    def __init__(self):
        r"""
        Documentation here
        """

        pass

    def set_content(self, file:str):
        r"""
        Documentation here
        """
        pass

    def serialize(self):
        r"""
        Documentation here
        """

        pass
