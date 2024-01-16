from ..helpers import Helpers
from .csv import CSVFormatter
from .olga import Olga
from .data_lake import DataLake
from .data_warehouse import DataWarehouse
import pandas as pd
import pickle
import struct
import os

olga = Olga()
csv = CSVFormatter()

@Helpers.as_airflow_tasks()
class IO:
    r"""
        Input/Output helps to extract data from different sources, like '.csv', '.tpl', 'sql databases', etc, into a Pandas
        DataFrame to be transformed in Airflow Pipelines.
    """

    @Helpers.check_airflow_task_args
    @staticmethod
    def read_csv(filepath:str, **kwargs)->pd.DataFrame:
        """
            Read a comma-separated values (csv) file into DataFrame.

            Also supports optionally iterating or breaking of the file into chunks.

            **Parameters**

            - **filepath:**  path string.

            Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.csv. 
            If you want to pass in a path object, pandas accepts any os.PathLike.

            By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.

            - **sep:** (str, default ',')

            Delimiter to use. If sep is None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator by Python's builtin sniffer tool, csv.Sniffer. In addition, separators longer than 1 character and different from '\s+' will be interpreted as regular expressions and will also force the use of the Python parsing engine. Note that regex delimiters are prone to ignoring quoted data. Regex example: '\r\t'.

            - **delimiter:** (str, default None)

            Alias for sep.

            - **header:** int, list of int, None, default 'infer'.

            Row number(s) to use as the column names, and the start of the data. Default behavior is to infer the column names: if no names are passed the behavior is identical to header=0 and column names are inferred from the first line of the file, if column names are passed explicitly then the behavior is identical to header=None. Explicitly pass header=0 to be able to replace existing names. The header can be a list of integers that specify row locations for a multi-index on the columns e.g. [0,1,3]. Intervening rows that are not specified will be skipped (e.g. 2 in this example is skipped). Note that this parameter ignores commented lines and empty lines if skip_blank_lines=True, so header=0 denotes the first line of data rather than the first line of the file.

            - **names:** array-like, optional

            List of column names to use. If the file contains a header row, then you should explicitly pass header=0 to override the column names. Duplicates in this list are not allowed.

            - **index_col:** int, str, sequence of int / str, or False, optional, default None.

            Column(s) to use as the row labels of the DataFrame, either given as string name or column index. If a sequence of int / str is given, a MultiIndex is used.

            > **Note:**
            >
            > index_col=False can be used to force pandas to not use the first column as the index, e.g. when you have a malformed file with delimiters at the end of each line.

            - **usecols:** list-like or callable, optional.

            Return a subset of the columns. If list-like, all elements must either be positional (i.e. integer indices into the document columns) or strings that correspond to column names provided either by the user in names or inferred from the document header row(s). If names are given, the document header row(s) are not taken into account. For example, a valid list-like usecols parameter would be [0, 1, 2] or ['foo', 'bar', 'baz']. Element order is ignored, so usecols=[0, 1] is the same as [1, 0]. To instantiate a DataFrame from data with element order preserved use pd.read_csv(data, usecols=['foo', 'bar'])[['foo', 'bar']] for columns in ['foo', 'bar'] order or pd.read_csv(data, usecols=['foo', 'bar'])[['bar', 'foo']] for ['bar', 'foo'] order.

            If callable, the callable function will be evaluated against the column names, returning names where the callable function evaluates to True. An example of a valid callable argument would be lambda x: x.upper() in ['AAA', 'BBB', 'DDD']. Using this parameter results in much faster parsing time and lower memory usage.

            - **dtype:** Type name or dict of column -> type, optional

            Data type for data or columns. E.g. {'a': np.float64, 'b': np.int32, 'c': 'Int64'} Use str or object together with suitable na_values settings to preserve and not interpret dtype. If converters are specified, they will be applied INSTEAD of dtype conversion.

            - **engine:** {'c', 'python', 'pyarrow'}, optional

            Parser engine to use. The C and pyarrow engines are faster, while the python engine is currently more feature-complete. Multithreading is currently only supported by the pyarrow engine.

            - **converters:** dict, optional

            Dict of functions for converting values in certain columns. Keys can either be integers or column labels.

            - **true_values:** list, optional

            Values to consider as True in addition to case-insensitive variants of “True”.

            - **false_values:** list, optional

            Values to consider as False in addition to case-insensitive variants of “False”.

            - **skipinitialspace:** bool, default False

            Skip spaces after delimiter.

            - **skiprows:** list-like, int or callable, optional

            Line numbers to skip (0-indexed) or number of lines to skip (int) at the start of the file.

            If callable, the callable function will be evaluated against the row indices, returning True if the row should be skipped and False otherwise. An example of a valid callable argument would be lambda x: x in [0, 2].

            - **skipfooter:** int, default 0

            Number of lines at bottom of file to skip (Unsupported with engine='c').

            - **nrows:** int, optional

            Number of rows of file to read. Useful for reading pieces of large files.

            - **na_values:** scalar, str, list-like, or dict, optional

            Additional strings to recognize as NA/NaN. If dict passed, specific per-column NA values. By default the following values are interpreted as NaN: '', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null'.

            - **keep_default_na:** bool, default True

            Whether or not to include the default NaN values when parsing the data. Depending on whether na_values is passed in, the behavior is as follows:

            > - If keep_default_na is True, and na_values are specified, na_values is appended to the default NaN values used for parsing.
            >
            > - If keep_default_na is True, and na_values are not specified, only the default NaN values are used for parsing.
            >
            > - If keep_default_na is False, and na_values are specified, only the NaN values specified na_values are used for parsing.
            >
            > - If keep_default_na is False, and na_values are not specified, no strings will be parsed as NaN.

            Note that if na_filter is passed in as False, the keep_default_na and na_values parameters will be ignored.

            - **na_filter:** bool, default True

            Detect missing value markers (empty strings and the value of na_values). In data without any NAs, passing na_filter=False can improve the performance of reading a large file.

            - **verbose:** bool, default False

            Indicate number of NA values placed in non-numeric columns.

            - **skip_blank_lines:** bool, default True

            If True, skip over blank lines rather than interpreting as NaN values.

            - **parse_dates:** bool or list of int or names or list of lists or dict, default False

            The behavior is as follows:

            > -  boolean. If True -> try parsing the index.
            >
            > - list of int or names. e.g. If [1, 2, 3] -> try parsing columns 1, 2, 3 each as a separate date column.
            >
            > - list of lists. e.g. If [[1, 3]] -> combine columns 1 and 3 and parse as a single date column.
            >
            > - dict, e.g. {'foo' : [1, 3]} -> parse columns 1, 3 as date and call result 'foo'

            If a column or index cannot be represented as an array of datetimes, say because of an unparsable value or a mixture of timezones, the column or index will be returned unaltered as an object data type. For non-standard datetime parsing, use pd.to_datetime after pd.read_csv.

            **Note:** A fast-path exists for iso8601-formatted dates.

            - **infer_datetime_format:** bool, default False

            If True and parse_dates is enabled, pandas will attempt to infer the format of the datetime strings in the columns, and if it can be inferred, switch to a faster method of parsing them. In some cases this can increase the parsing speed by 5-10x.
                
            - **keep_date_col:** bool, default False

            If True and parse_dates specifies combining multiple columns then keep the original columns.

            - **date_parser:** function, optional

            Function to use for converting a sequence of string columns to an array of datetime instances. The default uses dateutil.parser.parser to do the conversion. Pandas will try to call date_parser in three different ways, advancing to the next if an exception occurs: 1) Pass one or more arrays (as defined by parse_dates) as arguments; 2) concatenate (row-wise) the string values from the columns defined by parse_dates into a single array and pass that; and 3) call date_parser once for each row using one or more strings (corresponding to the columns defined by parse_dates) as arguments.

            - **date_format:** str or dict of column -> format, default None

            If used in conjunction with parse_dates, will parse dates according to this format. For anything more complex, please read in as object and then apply to_datetime() as-needed.

            - **dayfirst:** bool, default False

            DD/MM format dates, international and European format.

            - **cache_dates:** bool, default True

            If True, use a cache of unique, converted dates to apply the datetime conversion. May produce significant speed-up when parsing duplicate date strings, especially ones with timezone offsets.

            - **iterator:** bool, default False

            Return TextFileReader object for iteration or getting chunks with get_chunk().

            - **chunksize:** int, optional

            Return TextFileReader object for iteration. See the IO Tools docs for more information on iterator and chunksize.

            - **compression:** str or dict, default 'infer'

            For on-the-fly decompression of on-disk data. If 'infer' and 'filepath_or_buffer' is path-like, then detect compression from the following extensions: '.gz', '.bz2', '.zip', '.xz', '.zst', '.tar', '.tar.gz', '.tar.xz' or '.tar.bz2' (otherwise no compression). If using 'zip' or 'tar', the ZIP file must contain only one data file to be read in. Set to None for no decompression. Can also be a dict with key 'method' set to one of {'zip', 'gzip', 'bz2', 'zstd', 'tar'} and other key-value pairs are forwarded to zipfile.ZipFile, gzip.GzipFile, bz2.BZ2File, zstandard.ZstdDecompressor or tarfile.TarFile, respectively. As an example, the following could be passed for Zstandard decompression using a custom compression dictionary: compression={'method': 'zstd', 'dict_data': my_compression_dict}.

            - **thousands:** str, optional

            Thousands separator.

            - **decimal:** str, default '.'

            Character to recognize as decimal point (e.g. use ',' for European data).

            - **lineterminator:** str (length 1), optional

            Character to break file into lines. Only valid with C parser.

            - **quotechar:** str (length 1), optional

            The character used to denote the start and end of a quoted item. Quoted items can include the delimiter and it will be ignored.

            - **quoting:** int or csv.QUOTE_* instance, default 0

            Control field quoting behavior per csv.QUOTE_* constants. Use one of QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE (3).

            - **doublequote:** bool, default True

            When quotechar is specified and quoting is not QUOTE_NONE, indicate whether or not to interpret two consecutive quotechar elements INSIDE a field as a single quotechar element.

            - **escapechar:** str (length 1), optional

            One-character string used to escape other characters.

            - **comment:** str, optional

            Indicates remainder of line should not be parsed. If found at the beginning of a line, the line will be ignored altogether. This parameter must be a single character. Like empty lines (as long as skip_blank_lines=True), fully commented lines are ignored by the parameter header but not by skiprows. For example, if comment='#', parsing #empty\na,b,c\n1,2,3 with header=0 will result in 'a,b,c' being treated as the header.

            - **encoding:** str, optional, default “utf-8”

            Encoding to use for UTF when reading/writing (ex. 'utf-8'). [List of Python standard encodings](https://docs.python.org/3/library/codecs.html#standard-encodings).

            - **encoding_error:** sstr, optional, default “strict”

            How encoding errors are treated. [List of possible values](https://docs.python.org/3/library/codecs.html#error-handlers).

            - **dialect:** str or csv.Dialect, optional

            If provided, this parameter will override values (default or not) for the following parameters: delimiter, doublequote, escapechar, skipinitialspace, quotechar, and quoting. If it is necessary to override values, a ParserWarning will be issued. See csv.Dialect documentation for more details.

            - **on_bad_lines:** {'error', 'warn', 'skip'} or callable, default 'error'

            Specifies what to do upon encountering a bad line (a line with too many fields). Allowed values are :

            > - 'error', raise an Exception when a bad line is encountered.
            >
            > - 'warn', raise a warning when a bad line is encountered and skip that line.
            >
            > - 'skip', skip bad lines without raising or warning when they are encountered.

            - **delim_whitespace:** bool, default False

            Specifies whether or not whitespace (e.g. ' ' or '    ') will be used as the sep. Equivalent to setting sep='\s+'. If this option is set to True, nothing should be passed in for the delimiter parameter.

            - **low_memory:** bool, default True

            Internally process the file in chunks, resulting in lower memory use while parsing, but possibly mixed type inference. To ensure no mixed types either set False, or specify the type with the dtype parameter. Note that the entire file is read into a single DataFrame regardless, use the chunksize or iterator parameter to return the data in chunks. (Only valid with C parser).

            - **memory_map:** bool, default False

            If a filepath is provided for filepath_or_buffer, map the file object directly onto memory and access the data directly from there. Using this option can improve performance because there is no longer any I/O overhead.

            - **float_precision:** str, optional

            Specifies which converter the C engine should use for floating-point values. The options are None or 'high' for the ordinary converter, 'legacy' for the original lower precision pandas converter, and 'round_trip' for the round-trip converter.

            - **storage_options:** dict, optional

            Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer [here](https://pandas.pydata.org/docs/user_guide/io.html?highlight=storage_options#reading-writing-remote-files).

            - **dtype_backend:** {“numpy_nullable”, “pyarrow”}, defaults to NumPy backed DataFrames

            Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

            The dtype_backends are still experimential.

            **Returns**

            **DataFrame or TextFileReader**

            A comma-separated values (csv) file is returned as two-dimensional data structure with labeled axes.
        """
        return csv.read(filepath=filepath, **kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def read_olga(filepath:str, **kwargs)->pd.DataFrame:
        """
        Documentation here
        """
        return olga.read(filepath, **kwargs)

    @Helpers.check_airflow_task_args
    @staticmethod
    def read_pkl(filepath:str, **kwargs)->pd.DataFrame:
        """
        Documentation here
        """
        pass

    @Helpers.check_airflow_task_args
    @staticmethod
    def read_sql()->pd.DataFrame:
        """
        Documentation here
        """
        pass

    @Helpers.check_airflow_task_args
    @staticmethod
    def save_append_to_pkl(filepath, filename, data_dict):
    
        pickled_data = pickle.dumps(data_dict)
        with open(os.path.join(filepath, filename), 'ab') as f:
            f.write(struct.pack("I", len(pickled_data)))  # 4 bytes to indicate the size of the pickled data
            f.write(pickled_data)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def read_dicts(filepath, filename):
        dicts = []
        with open(os.path.join(filepath, filename), 'rb') as f:
            while True:
                size_data = f.read(4)
                if not size_data:
                    break
                size = struct.unpack("I", size_data)[0]
                pickled_data = f.read(size)
                dicts.append(pickle.loads(pickled_data))
        return dicts

    @Helpers.check_airflow_task_args
    @staticmethod
    def read_data_lake(mongo_user:str, mongo_password:str, host:str, port:int = 27017, **kwargs):
        data_lake = DataLake(mongo_user, mongo_password, host, port)
        return data_lake.read(**kwargs)
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def save_data_warehouse_from_simulation(tpl_df, genkey, tpl_serialized, meta, dw_user, dw_password, **kwargs):

        data_warehouse = DataWarehouse(dw_user, dw_password, '3.139.233.232', 5432, 'warehouse', 'postgresql')

        timestamp_list = []
        for i in pd.to_datetime(tpl_df['TIME_SERIES_S'], unit='s').items():
            timestamp_list.append(i[1].strftime('%Y/%m/%d %H:%M:%S.%f'))
        
        data_warehouse.send_bulk_simulation_timestamps_blob(timestamp_list)
        
        data_warehouse.save_and_create_case(meta['leak_size'], meta['leak_location'], meta['fluid'], meta['stroke'], meta['operation_state'], meta['failure'], meta['line'], meta['terminal'], meta['title'])
        # total_tags = data_warehouse.save_tags(tpl_df.columns)
        data_warehouse.save_tags_and_send_blobs(tpl_df, meta['title'])
        # for i in total_tags:
        #     tag_values = []
        #     for j in range(len(timestamp_list)):            
                
        #         tag_values.append({'timestamp':timestamp_list[j], 'tag': i, 'case': meta['title'], 'value': tpl_df[i][j]})

        #     data_warehouse.send_bulk_tag_values_blob(tag_values)


    @Helpers.check_airflow_task_args
    @staticmethod
    def save_data_warehouse_from_field(tpl_df, genkey, tpl_serialized):
        
        pass

