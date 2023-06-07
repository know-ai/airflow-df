import pandas as pd

class TPL:
    r"""
    Documentation Here
    """

    def __init__(self):

        self.raw_file = None
        self.info = Info()

    def set_info(self, file:str):
        r"""
        Set all info attributes of tpl file in each class attribute.

        **Parameters**

        - **file:** (str) raw tpl file as string.

        **Attributes to set**

        - **version:** (str) Olga version with which the file was generated
        - **input_file:** (str) Genkey file name binded to this tpl file
        - **pvt_file:** (str) Fluid property file name
        - **date:** (str) TPL file generation date
        - **project:** (str) Project name
        - **title:** (str) Simulation title name
        - **author:** (str) Responsible for the simulation
        - **network:** (int) If 1, is a pipeline, > 1, is a network
        - **geometry:** (str) Unit for pipeline length
        - **branch:** (str) Branch name
        """
        self.info.set_info(file)

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
    Stores the attributes in the information section inside a tpl file
    for example
    >>> 'OLGA 2017.2.0.107'
    >>> INPUT FILE
    >>> 'SF_SH_D0_R0.genkey'
    >>> PVT FILE
    >>> '../../../00 Nuevos fluidos/Diesel_1.tab' 
    >>> DATE
    >>> '23-06-06 15:12:27'
    >>> PROJECT
    >>> 'Supe'
    >>> TITLE
    >>> ''
    >>> AUTHOR
    >>> 'Jesus E Varajas'
    >>> NETWORK
    >>> 1
    >>> GEOMETRY ' (M)  '
    >>> BRANCH
    >>> 'PIPELINE'

    As class attributes
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
        Set all info attributes of tpl file in each class attribute.

        **Parameters**

        - **file:** (str) raw tpl file as string.

        **Attributes to set**

        - **version:** (str) Olga version with which the file was generated
        - **input_file:** (str) Genkey file name binded to this tpl file
        - **pvt_file:** (str) Fluid property file name
        - **date:** (str) TPL file generation date
        - **project:** (str) Project name
        - **title:** (str) Simulation title name
        - **author:** (str) Responsible for the simulation
        - **network:** (int) If 1, is a pipeline, > 1, is a network
        - **geometry:** (str) Unit for pipeline length
        - **branch:** (str) Branch name
        """
        pass

    def serialize(self):
        r"""
        Serializes all information attributes of the Olga file

        **Returns**

        - **attrs:** (dict)
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
