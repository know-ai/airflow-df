import sys, os
# These code lines is to avoid import relative error
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from helpers import as_airflow_tasks
from .csv import CSVFormatter

@as_airflow_tasks()
class IO:
    r"""
    Documentation here
    """
    @staticmethod
    def read_csv(filepath:str, **kwargs):
        r"""
        Read a comma-separated values (csv) file into DataFrame.

        Also supports optionally iterating or breaking of the file into chunks.

        **Parameters**

        - *filepath:*  path object
        Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.csv. 
        If you want to pass in a path object, pandas accepts any os.PathLike.
        By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.

        - *sep:* (str, default ',')
        Delimiter to use. If sep is None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator by Python’s builtin sniffer tool, csv.Sniffer. In addition, separators longer than 1 character and different from '\s+' will be interpreted as regular expressions and will also force the use of the Python parsing engine. Note that regex delimiters are prone to ignoring quoted data. Regex example: '\r\t'.

        - *delimiter:* (str, default None)
        Alias for sep.
        """
        if "config" in kwargs:

            config = kwargs['config']
            
        return CSVFormatter.read(filepath=filepath, **kwargs)