import pandas as pd


class CSVFormatter:
    r"""
    Documentation here
    """

    @staticmethod
    def read(filepath, **kwargs)->pd.DataFrame:
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
        df = pd.read_csv(filepath, **kwargs)

        print(df.info())
        print(df.head())
        
        return df