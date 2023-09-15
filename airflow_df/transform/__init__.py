from ..helpers import Helpers
import pandas as pd
import re
import numpy as np
import math
# @Helpers.as_airflow_tasks()
class Transform:
    """Documentation here

    """
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def rename(
        df,
        *,
        columns:None=None,
        errors="ignore"
    )->pd.DataFrame | None:
        """
            Rename columns or index labels.

            Function / dict values must be unique (1-to-1). Labels not contained in
            a dict / Series will be left as-is. Extra labels listed don't throw an
            error.

            See the :ref:`user guide <basics.rename>` for more.

            Parameters
            ----------
            mapper : dict-like or function
                Dict-like or function transformations to apply to
                that axis' values. Use either ``mapper`` and ``axis`` to
                specify the axis to target with ``mapper``, or ``index`` and
                ``columns``.
            index : dict-like or function
                Alternative to specifying axis (``mapper, axis=0``
                is equivalent to ``index=mapper``).
            columns : dict-like or function
                Alternative to specifying axis (``mapper, axis=1``
                is equivalent to ``columns=mapper``).
            axis : {0 or 'index', 1 or 'columns'}, default 0
                Axis to target with ``mapper``. Can be either the axis name
                ('index', 'columns') or number (0, 1). The default is 'index'.
            copy : bool, default True
                Also copy underlying data.
            inplace : bool, default False
                Whether to modify the DataFrame rather than creating a new one.
                If True then value of copy is ignored.
            level : int or level name, default None
                In case of a MultiIndex, only rename labels in the specified
                level.
            errors : {'ignore', 'raise'}, default 'ignore'
                If 'raise', raise a `KeyError` when a dict-like `mapper`, `index`,
                or `columns` contains labels that are not present in the Index
                being transformed.
                If 'ignore', existing keys will be renamed and extra keys will be
                ignored.

            Returns
            -------
            DataFrame or None
                DataFrame with the renamed axis labels or None if ``inplace=True``.

            Raises
            ------
            KeyError
                If any of the labels is not found in the selected axis and
                "errors='raise'".

            See Also
            --------
            DataFrame.rename_axis : Set the name of the axis.

            Examples
            --------
            ``DataFrame.rename`` supports two calling conventions

            * ``(index=index_mapper, columns=columns_mapper, ...)``
            * ``(mapper, axis={'index', 'columns'}, ...)``

            We *highly* recommend using keyword arguments to clarify your
            intent.

            Rename columns using a mapping:

            >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
            >>> df.rename(columns={"A": "a", "B": "c"})
            a  c
            0  1  4
            1  2  5
            2  3  6

            Rename index using a mapping:

            >>> df.rename(index={0: "x", 1: "y", 2: "z"})
            A  B
            x  1  4
            y  2  5
            z  3  6

            Cast index labels to a different type:

            >>> df.index
            RangeIndex(start=0, stop=3, step=1)
            >>> df.rename(index=str).index
            Index(['0', '1', '2'], dtype='object')

            >>> df.rename(columns={"A": "a", "B": "b", "C": "c"}, errors="raise")
            Traceback (most recent call last):
            KeyError: ['C'] not found in axis

            Using axis-style parameters:

            >>> df.rename(str.lower, axis='columns')
            a  b
            0  1  4
            1  2  5
            2  3  6

            >>> df.rename({1: 2, 2: 4}, axis='index')
            A  B
            0  1  4
            2  2  5
            4  3  6
        """

        return df.rename(
            columns=columns,
            errors=errors
        )
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def drop(
        df,
        *,
        columns:None=None,
        errors="ignore"
    ) -> pd.DataFrame | None:
        """
            Drop specified labels from rows or columns.

            Remove rows or columns by specifying label names and corresponding
            axis, or by specifying directly index or column names. When using a
            multi-index, labels on different levels can be removed by specifying
            the level. See the :ref:`user guide <advanced.shown_levels>`
            for more information about the now unused levels.

            Parameters
            ----------
            labels : single label or list-like
                Index or column labels to drop. A tuple will be used as a single
                label and not treated as a list-like.
            axis : {0 or 'index', 1 or 'columns'}, default 0
                Whether to drop labels from the index (0 or 'index') or
                columns (1 or 'columns').
            index : single label or list-like
                Alternative to specifying axis (``labels, axis=0``
                is equivalent to ``index=labels``).
            columns : single label or list-like
                Alternative to specifying axis (``labels, axis=1``
                is equivalent to ``columns=labels``).
            level : int or level name, optional
                For MultiIndex, level from which the labels will be removed.
            inplace : bool, default False
                If False, return a copy. Otherwise, do operation
                inplace and return None.
            errors : {'ignore', 'raise'}, default 'raise'
                If 'ignore', suppress error and only existing labels are
                dropped.

            Returns
            -------
            DataFrame or None
                DataFrame without the removed index or column labels or
                None if ``inplace=True``.

            Raises
            ------
            KeyError
                If any of the labels is not found in the selected axis.

            See Also
            --------
            DataFrame.loc : Label-location based indexer for selection by label.
            DataFrame.dropna : Return DataFrame with labels on given axis omitted
                where (all or any) data are missing.
            DataFrame.drop_duplicates : Return DataFrame with duplicate rows
                removed, optionally only considering certain columns.
            Series.drop : Return Series with specified index labels removed.

            Examples
            --------
            >>> df = pd.DataFrame(np.arange(12).reshape(3, 4),
            ...                   columns=['A', 'B', 'C', 'D'])
            >>> df
            A  B   C   D
            0  0  1   2   3
            1  4  5   6   7
            2  8  9  10  11

            Drop columns

            >>> df.drop(['B', 'C'], axis=1)
            A   D
            0  0   3
            1  4   7
            2  8  11

            >>> df.drop(columns=['B', 'C'])
            A   D
            0  0   3
            1  4   7
            2  8  11

            Drop a row by index

            >>> df.drop([0, 1])
            A  B   C   D
            2  8  9  10  11

            Drop columns and/or rows of MultiIndex DataFrame

            >>> midx = pd.MultiIndex(levels=[['lama', 'cow', 'falcon'],
            ...                              ['speed', 'weight', 'length']],
            ...                      codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2],
            ...                             [0, 1, 2, 0, 1, 2, 0, 1, 2]])
            >>> df = pd.DataFrame(index=midx, columns=['big', 'small'],
            ...                   data=[[45, 30], [200, 100], [1.5, 1], [30, 20],
            ...                         [250, 150], [1.5, 0.8], [320, 250],
            ...                         [1, 0.8], [0.3, 0.2]])
            >>> df
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
                    length  1.5     1.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
                    length  1.5     0.8
            falcon  speed   320.0   250.0
                    weight  1.0     0.8
                    length  0.3     0.2

            Drop a specific index combination from the MultiIndex
            DataFrame, i.e., drop the combination ``'falcon'`` and
            ``'weight'``, which deletes only the corresponding row

            >>> df.drop(index=('falcon', 'weight'))
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
                    length  1.5     1.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
                    length  1.5     0.8
            falcon  speed   320.0   250.0
                    length  0.3     0.2

            >>> df.drop(index='cow', columns='small')
                            big
            lama    speed   45.0
                    weight  200.0
                    length  1.5
            falcon  speed   320.0
                    weight  1.0
                    length  0.3

            >>> df.drop(index='length', level=1)
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
            falcon  speed   320.0   250.0
                    weight  1.0     0.8
        """
        __columns = df.columns.values.tolist()
                
        __not_in_df_columns = [column for column in columns if column not in __columns]
        
        if(len(__not_in_df_columns)>0):
            raise IndexError(f'This labels are not in the dataframe {", ".join(not_in_df_columns)}')

        return df.drop(
            columns=columns,
            errors=errors,
        )
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def keep(df,
        labels=None,
        *,
        axis:bool=0,
        index:bool=None,
        columns:bool=None,
        level=None,
        inplace:bool=False,
        errors="raise",
    ) -> pd.DataFrame | None:
        """
            Keeps specified labels from rows or columns.

            Remove rows or columns differents by specifying label names and corresponding
            axis, or by specifying directly index or column names. When using a
            multi-index, labels on different levels can be removed by specifying
            the level. See the :ref:`user guide <advanced.shown_levels>`
            for more information about the now unused levels.

            Parameters
            ----------
            labels : single label or list-like
                Index or column labels to keep. A tuple will be used as a single
                label and not treated as a list-like.
            axis : {0 or 'index', 1 or 'columns'}, default 0
                Whether to drop labels from the index (0 or 'index') or
                columns (1 or 'columns').
            index : single label or list-like
                Alternative to specifying axis (``labels, axis=0``
                is equivalent to ``index=labels``).
            columns : single label or list-like
                Alternative to specifying axis (``labels, axis=1``
                is equivalent to ``columns=labels``).
            level : int or level name, optional
                For MultiIndex, level from which the labels will be removed.
            inplace : bool, default False
                If False, return a copy. Otherwise, do operation
                inplace and return None.
            errors : {'ignore', 'raise'}, default 'raise'
                If 'ignore', suppress error and only existing labels are
                dropped.

            Returns
            -------
            DataFrame or None
                DataFrame without the removed index or column labels or
                None if ``inplace=True``.

            Raises
            ------
            KeyError
                If any of the labels is not found in the selected axis.

            See Also
            --------
            DataFrame.loc : Label-location based indexer for selection by label.
            DataFrame.dropna : Return DataFrame with labels on given axis omitted
                where (all or any) data are missing.
            DataFrame.drop_duplicates : Return DataFrame with duplicate rows
                removed, optionally only considering certain columns.
            Series.drop : Return Series with specified index labels removed.

            Examples
            --------
            >>> df = pd.DataFrame(np.arange(12).reshape(3, 4),
            ...                   columns=['A', 'B', 'C', 'D'])
            >>> df
            A  B   C   D
            0  0  1   2   3
            1  4  5   6   7
            2  8  9  10  11

            Drop columns

            >>> df.drop(['B', 'C'], axis=1)
            A   D
            0  0   3
            1  4   7
            2  8  11

            >>> df.drop(columns=['B', 'C'])
            A   D
            0  0   3
            1  4   7
            2  8  11

            Drop a row by index

            >>> df.drop([0, 1])
            A  B   C   D
            2  8  9  10  11

            Drop columns and/or rows of MultiIndex DataFrame

            >>> midx = pd.MultiIndex(levels=[['lama', 'cow', 'falcon'],
            ...                              ['speed', 'weight', 'length']],
            ...                      codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2],
            ...                             [0, 1, 2, 0, 1, 2, 0, 1, 2]])
            >>> df = pd.DataFrame(index=midx, columns=['big', 'small'],
            ...                   data=[[45, 30], [200, 100], [1.5, 1], [30, 20],
            ...                         [250, 150], [1.5, 0.8], [320, 250],
            ...                         [1, 0.8], [0.3, 0.2]])
            >>> df
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
                    length  1.5     1.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
                    length  1.5     0.8
            falcon  speed   320.0   250.0
                    weight  1.0     0.8
                    length  0.3     0.2

            Drop a specific index combination from the MultiIndex
            DataFrame, i.e., drop the combination ``'falcon'`` and
            ``'weight'``, which deletes only the corresponding row

            >>> df.drop(index=('falcon', 'weight'))
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
                    length  1.5     1.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
                    length  1.5     0.8
            falcon  speed   320.0   250.0
                    length  0.3     0.2

            >>> df.drop(index='cow', columns='small')
                            big
            lama    speed   45.0
                    weight  200.0
                    length  1.5
            falcon  speed   320.0
                    weight  1.0
                    length  0.3

            >>> df.drop(index='length', level=1)
                            big     small
            lama    speed   45.0    30.0
                    weight  200.0   100.0
            cow     speed   30.0    20.0
                    weight  250.0   150.0
            falcon  speed   320.0   250.0
                    weight  1.0     0.8
        """

        __df_columns = df.columns.values.tolist()
                
        not_in_df_columns = [column for column in columns if column not in __df_columns]
        
        if(len(not_in_df_columns)>0):
            raise IndexError(f'This labels are not in the dataframe {", ".join(not_in_df_columns)}')

        __df_columns = [column for column in __df_columns if column not in columns]


        return df.drop(
            index=index,
            columns=__df_columns,
            level=level,
            errors=errors,
        )
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def resample(
        df:pd.DataFrame,
        rule=None, 
        axis=0, 
        closed=None, 
        label=None, 
        convention='start', 
        kind=None, 
        on=None, 
        level=None, 
        origin='start_day', 
        offset=None, 
        group_keys=False
    )->pd.DataFrame:
        """
            Resample time-series data.

            Convenience method for frequency conversion and resampling of time series.
            The object must have a datetime-like index (`DatetimeIndex`, `PeriodIndex`,
            or `TimedeltaIndex`), or the caller must pass the label of a datetime-like
            series/index to the ``on``/``level`` keyword parameter.

            **Parameters**

            - **rule:** (DateOffset, Timedelta or str)

                The offset string or object representing target conversion.

            - **axis:** {{0 or 'index', 1 or 'columns'}}, default 0

                Which axis to use for up- or down-sampling. For `Series` this parameter 
                is unused and defaults to 0. Must be
                `DatetimeIndex`, `TimedeltaIndex` or `PeriodIndex`.

            - **closed:** {{'right', 'left'}}, default None

                Which side of bin interval is closed. The default is 'left'
                for all frequency offsets except for 'M', 'A', 'Q', 'BM',
                'BA', 'BQ', and 'W' which all have a default of 'right'.

            - **label : {{'right', 'left'}}, default None
                Which bin edge label to label bucket with. The default is 'left'
                for all frequency offsets except for 'M', 'A', 'Q', 'BM',
                'BA', 'BQ', and 'W' which all have a default of 'right'.

            - **convention:** {{'start', 'end', 's', 'e'}}, default 'start'

                For `PeriodIndex` only, controls whether to use the start or
                end of `rule`.

            - **kind:** {{'timestamp', 'period'}}, optional, default None

                Pass 'timestamp' to convert the resulting index to a
                `DateTimeIndex` or 'period' to convert it to a `PeriodIndex`.
                By default the input representation is retained.

            - **on:** str, optional

                For a DataFrame, column to use instead of index for resampling.
                Column must be datetime-like.

            - **level:** str or int, optional

                For a MultiIndex, level (name or number) to use for
                resampling. `level` must be datetime-like.

            - **origin:** Timestamp or str, default 'start_day'

                The timestamp on which to adjust the grouping. The timezone of origin
                must match the timezone of the index.
                If string, must be one of the following:

                - 'epoch': `origin` is 1970-01-01
                - 'start': `origin` is the first value of the timeseries
                - 'start_day': `origin` is the first day at midnight of the timeseries
                - 'end': `origin` is the last value of the timeseries
                - 'end_day': `origin` is the ceiling midnight of the last day


            - **offset:** Timedelta or str, default is None

                An offset timedelta added to the origin.

            - **group_keys:** bool, default False

                Whether to include the group keys in the result index when using
                ``.apply()`` on the resampled object.

                    Not specifying ``group_keys`` will retain values-dependent behavior
                    from pandas 1.4 and earlier (see :ref:`pandas 1.5.0 Release notes
                    <whatsnew_150.enhancements.resample_group_keys>` for examples).

                    ``group_keys`` now defaults to ``False``.

            Returns
            -------
            pandas.core.Resampler
                :class:`~pandas.core.Resampler` object.

            See Also
            --------
            Series.resample : Resample a Series.
            DataFrame.resample : Resample a DataFrame.
            groupby : Group {klass} by mapping, function, label, or list of labels.
            asfreq : Reindex a {klass} with the given frequency without grouping.

            Notes
            -----
            See the `user guide
            <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#resampling>`__
            for more.

            To learn more about the offset strings, please see `this link
            <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects>`__.

            Examples
            --------
            Start by creating a series with 9 one minute timestamps.

            >>> index = pd.date_range('1/1/2000', periods=9, freq='T')
            >>> series = pd.Series(range(9), index=index)
            >>> series
            2000-01-01 00:00:00    0
            2000-01-01 00:01:00    1
            2000-01-01 00:02:00    2
            2000-01-01 00:03:00    3
            2000-01-01 00:04:00    4
            2000-01-01 00:05:00    5
            2000-01-01 00:06:00    6
            2000-01-01 00:07:00    7
            2000-01-01 00:08:00    8
            Freq: T, dtype: int64

            Downsample the series into 3 minute bins and sum the values
            of the timestamps falling into a bin.

            >>> series.resample('3T').sum()
            2000-01-01 00:00:00     3
            2000-01-01 00:03:00    12
            2000-01-01 00:06:00    21
            Freq: 3T, dtype: int64

            Downsample the series into 3 minute bins as above, but label each
            bin using the right edge instead of the left. Please note that the
            value in the bucket used as the label is not included in the bucket,
            which it labels. For example, in the original series the
            bucket ``2000-01-01 00:03:00`` contains the value 3, but the summed
            value in the resampled bucket with the label ``2000-01-01 00:03:00``
            does not include 3 (if it did, the summed value would be 6, not 3).
            To include this value close the right side of the bin interval as
            illustrated in the example below this one.

            >>> series.resample('3T', label='right').sum()
            2000-01-01 00:03:00     3
            2000-01-01 00:06:00    12
            2000-01-01 00:09:00    21
            Freq: 3T, dtype: int64

            Downsample the series into 3 minute bins as above, but close the right
            side of the bin interval.

            >>> series.resample('3T', label='right', closed='right').sum()
            2000-01-01 00:00:00     0
            2000-01-01 00:03:00     6
            2000-01-01 00:06:00    15
            2000-01-01 00:09:00    15
            Freq: 3T, dtype: int64

            Upsample the series into 30 second bins.

            >>> series.resample('30S').asfreq()[0:5]   # Select first 5 rows
            2000-01-01 00:00:00   0.0
            2000-01-01 00:00:30   NaN
            2000-01-01 00:01:00   1.0
            2000-01-01 00:01:30   NaN
            2000-01-01 00:02:00   2.0
            Freq: 30S, dtype: float64

            Upsample the series into 30 second bins and fill the ``NaN``
            values using the ``ffill`` method.

            >>> series.resample('30S').ffill()[0:5]
            2000-01-01 00:00:00    0
            2000-01-01 00:00:30    0
            2000-01-01 00:01:00    1
            2000-01-01 00:01:30    1
            2000-01-01 00:02:00    2
            Freq: 30S, dtype: int64

            Upsample the series into 30 second bins and fill the
            ``NaN`` values using the ``bfill`` method.

            >>> series.resample('30S').bfill()[0:5]
            2000-01-01 00:00:00    0
            2000-01-01 00:00:30    1
            2000-01-01 00:01:00    1
            2000-01-01 00:01:30    2
            2000-01-01 00:02:00    2
            Freq: 30S, dtype: int64

            Pass a custom function via ``apply``

            >>> def custom_resampler(arraylike):
            ...     return np.sum(arraylike) + 5
            ...
            >>> series.resample('3T').apply(custom_resampler)
            2000-01-01 00:00:00     8
            2000-01-01 00:03:00    17
            2000-01-01 00:06:00    26
            Freq: 3T, dtype: int64

            For a Series with a PeriodIndex, the keyword `convention` can be
            used to control whether to use the start or end of `rule`.

            Resample a year by quarter using 'start' `convention`. Values are
            assigned to the first quarter of the period.

            >>> s = pd.Series([1, 2], index=pd.period_range('2012-01-01',
            ...                                             freq='A',
            ...                                             periods=2))
            >>> s
            2012    1
            2013    2
            Freq: A-DEC, dtype: int64
            >>> s.resample('Q', convention='start').asfreq()
            2012Q1    1.0
            2012Q2    NaN
            2012Q3    NaN
            2012Q4    NaN
            2013Q1    2.0
            2013Q2    NaN
            2013Q3    NaN
            2013Q4    NaN
            Freq: Q-DEC, dtype: float64

            Resample quarters by month using 'end' `convention`. Values are
            assigned to the last month of the period.

            >>> q = pd.Series([1, 2, 3, 4], index=pd.period_range('2018-01-01',
            ...                                                   freq='Q',
            ...                                                   periods=4))
            >>> q
            2018Q1    1
            2018Q2    2
            2018Q3    3
            2018Q4    4
            Freq: Q-DEC, dtype: int64
            >>> q.resample('M', convention='end').asfreq()
            2018-03    1.0
            2018-04    NaN
            2018-05    NaN
            2018-06    2.0
            2018-07    NaN
            2018-08    NaN
            2018-09    3.0
            2018-10    NaN
            2018-11    NaN
            2018-12    4.0
            Freq: M, dtype: float64

            For DataFrame objects, the keyword `on` can be used to specify the
            column instead of the index for resampling.

            >>> d = {{'price': [10, 11, 9, 13, 14, 18, 17, 19],
            ...      'volume': [50, 60, 40, 100, 50, 100, 40, 50]}}
            >>> df = pd.DataFrame(d)
            >>> df['week_starting'] = pd.date_range('01/01/2018',
            ...                                     periods=8,
            ...                                     freq='W')
            >>> df
                price  volume week_starting
            0     10      50    2018-01-07
            1     11      60    2018-01-14
            2      9      40    2018-01-21
            3     13     100    2018-01-28
            4     14      50    2018-02-04
            5     18     100    2018-02-11
            6     17      40    2018-02-18
            7     19      50    2018-02-25
            >>> df.resample('M', on='week_starting').mean()
                            price  volume
            week_starting
            2018-01-31     10.75    62.5
            2018-02-28     17.00    60.0

            For a DataFrame with MultiIndex, the keyword `level` can be used to
            specify on which level the resampling needs to take place.

            >>> days = pd.date_range('1/1/2000', periods=4, freq='D')
            >>> d2 = {{'price': [10, 11, 9, 13, 14, 18, 17, 19],
            ...       'volume': [50, 60, 40, 100, 50, 100, 40, 50]}}
            >>> df2 = pd.DataFrame(
            ...     d2,
            ...     index=pd.MultiIndex.from_product(
            ...         [days, ['morning', 'afternoon']]
            ...     )
            ... )
            >>> df2
                                    price  volume
            2000-01-01 morning       10      50
                        afternoon     11      60
            2000-01-02 morning        9      40
                        afternoon     13     100
            2000-01-03 morning       14      50
                        afternoon     18     100
            2000-01-04 morning       17      40
                        afternoon     19      50
            >>> df2.resample('D', level=0).sum()
                        price  volume
            2000-01-01     21     110
            2000-01-02     22     140
            2000-01-03     32     150
            2000-01-04     36      90

            If you want to adjust the start of the bins based on a fixed timestamp:

            >>> start, end = '2000-10-01 23:30:00', '2000-10-02 00:30:00'
            >>> rng = pd.date_range(start, end, freq='7min')
            >>> ts = pd.Series(np.arange(len(rng)) * 3, index=rng)
            >>> ts
            2000-10-01 23:30:00     0
            2000-10-01 23:37:00     3
            2000-10-01 23:44:00     6
            2000-10-01 23:51:00     9
            2000-10-01 23:58:00    12
            2000-10-02 00:05:00    15
            2000-10-02 00:12:00    18
            2000-10-02 00:19:00    21
            2000-10-02 00:26:00    24
            Freq: 7T, dtype: int64

            >>> ts.resample('17min').sum()
            2000-10-01 23:14:00     0
            2000-10-01 23:31:00     9
            2000-10-01 23:48:00    21
            2000-10-02 00:05:00    54
            2000-10-02 00:22:00    24
            Freq: 17T, dtype: int64

            >>> ts.resample('17min', origin='epoch').sum()
            2000-10-01 23:18:00     0
            2000-10-01 23:35:00    18
            2000-10-01 23:52:00    27
            2000-10-02 00:09:00    39
            2000-10-02 00:26:00    24
            Freq: 17T, dtype: int64

            >>> ts.resample('17min', origin='2000-01-01').sum()
            2000-10-01 23:24:00     3
            2000-10-01 23:41:00    15
            2000-10-01 23:58:00    45
            2000-10-02 00:15:00    45
            Freq: 17T, dtype: int64

            If you want to adjust the start of the bins with an `offset` Timedelta, the two
            following lines are equivalent:

            >>> ts.resample('17min', origin='start').sum()
            2000-10-01 23:30:00     9
            2000-10-01 23:47:00    21
            2000-10-02 00:04:00    54
            2000-10-02 00:21:00    24
            Freq: 17T, dtype: int64

            >>> ts.resample('17min', offset='23h30min').sum()
            2000-10-01 23:30:00     9
            2000-10-01 23:47:00    21
            2000-10-02 00:04:00    54
            2000-10-02 00:21:00    24
            Freq: 17T, dtype: int64

            If you want to take the largest Timestamp as the end of the bins:

            >>> ts.resample('17min', origin='end').sum()
            2000-10-01 23:35:00     0
            2000-10-01 23:52:00    18
            2000-10-02 00:09:00    27
            2000-10-02 00:26:00    63
            Freq: 17T, dtype: int64

            In contrast with the `start_day`, you can use `end_day` to take the ceiling
            midnight of the largest Timestamp as the end of the bins and drop the bins
            not containing data:

            >>> ts.resample('17min', origin='end_day').sum()
            2000-10-01 23:38:00     3
            2000-10-01 23:55:00    15
            2000-10-02 00:12:00    45
            2000-10-02 00:29:00    45
            Freq: 17T, dtype: int64
        """

        return df.resample(
            rule,
            axis=axis, 
            closed=closed, 
            label=label, 
            convention=convention, 
            kind=kind, 
            on=on, 
            level=level, 
            origin=origin, 
            offset=offset, 
            group_keys=group_keys
        )
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def reset_index(df:pd.DataFrame)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df.reset_index()
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def convert_to_float(df:pd.DataFrame)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def set_datetime_index(df:pd.DataFrame)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def info(df:pd.DataFrame)->pd.DataFrame:
        r"""
        Documentation here
        """
        print(df.info())
        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def add_columns(df:pd.DataFrame)->pd.DataFrame:
        r"""
        Documentation here
        """

        return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def concatenate_three_parts(input_string)->str:
        """
        This concatenates the three "special" parts of the column names from a TPL
        It transforms names as such as SSP 'POSITION:' 'POS-19M' '(M/S)' 'Speed of sound in fluid'
        to SSP_POSITION_POS@19M_Speed_of_sound_in_fluid_M/S.

        It takes the label name the string position and the position for the first  part
        then adds the name of the variable and at the end appends the measure unit.
        
        for the common uses makes this 
        LABEL_NAME_POS@XXM_Variable_name_MEASURE_UNIT

        If there is not any variable name nor measure unit, it just doesn't add it
        """
        # Split the input string into parts using single quotes as separators
        unit_match = re.search(r'\((.*?)\)', input_string)
        unit = None
        if unit_match:
            # Extract and return the text inside parentheses
            unit = unit_match.group(1)
        
        parts = input_string.split("(")
        parts_2 = input_string.split(")")

        parts = parts[0]
        parts_2 = parts_2[-1]
        parts = parts.replace("'"," ")
        parts_2 = parts_2.replace("'"," ")

        parts_2 = parts_2.replace(":"," ")
        parts = parts.replace(":"," ")

        parts = ' '.join(parts.split())
        parts_2 = ' '.join(parts_2.split())

        
        parts = parts.replace(" ", "_") 
        parts_2 = parts_2.replace(" ", "_") 
        if('POS' in parts):
            parts = parts.replace("-", "@")
        else:
            parts = parts.replace("-", "_")

        if(parts_2):
            parts = f"{parts}_{parts_2}"
        if(unit):
            parts = f"{parts}_{unit}" 
        parts = parts.strip('-')
        parts = parts.strip('_')
        
        # Extract the first three parts and join them with underscores
        concatenated_parts = parts
        return concatenated_parts

    @Helpers.check_airflow_task_args
    @staticmethod
    def standarize_column_names(df:pd.DataFrame, *args, **kwargs)->pd.DataFrame | None:

        column_names = df.columns.to_list()
        column_standarize_names = dict()
        for i in column_names:
            column_standarize_names[i] = Transform.concatenate_three_parts(i)
        df = Transform.rename(df, columns=column_standarize_names)
        return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_size(df, genkey):
        
        LEAK_SIZE = None

        for i in genkey['Network Component']:
            if 'NETWORKCOMPONENT' in i:
                if 'TYPE' in i['NETWORKCOMPONENT']:
                    if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):   
                        if('LEAK' in i):
                            LEAK_SIZE =  i['LEAK']['DIAMETER']['VALUE'][0]

        if(LEAK_SIZE is None):
            raise ValueError('There is no leak size in this genkey simulation.')
        df['LEAK_SIZE'] = LEAK_SIZE

        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_location(df, genkey):
    
        LEAK_LOCATION = None

        for i in genkey['Network Component']:
            if 'NETWORKCOMPONENT' in i:
                if 'TYPE' in i['NETWORKCOMPONENT']:
                    if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):   
                        if('LEAK' in i):

                            LEAK_LOCATION =  i['LEAK']['ABSPOSITION']['VALUE'][0]

        if(LEAK_LOCATION is None):
            raise ValueError('There is no leak location in this genkey simulation.')

        df['LEAK_LOCATION'] = LEAK_LOCATION

        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_status(df, genkey):
        if('TIME_SERIES_S' in df):
            for i in genkey['Network Component']:
                if 'LABEL' in i['PARAMETERS']:
                    if('Control-Leak' == i['PARAMETERS']['LABEL']):   
                        setpoint = i['PARAMETERS']['SETPOINT']
                        time_values = i['PARAMETERS']['TIME']['VALUE']
                        break
            if(type(setpoint)==int):
                df['LEAK_STATUS'] = 'NO LEAK'

            elif(type(setpoint)==list):
                leak_ranges = list()
                for i in range(len(setpoint)):
                    if(setpoint[i]):
                        if(i+1 < len(time_values)):
                            leak_ranges.append((time_values[i], time_values[i+1]))
                        else:
                            leak_ranges.append((time_values[i], float('inf')))
                df['LEAK_STATUS'] = 'NO LEAK'
                for i in leak_ranges:
                    df.loc[(i[0]<df['TIME_SERIES_S'])&(df['TIME_SERIES_S']<i[1]), 'LEAK_STATUS'] = 'LEAK'
            
            return df 
        else:
            raise KeyError("You must have the column TIME_SERIES_S to do this transformation.\nPlease, don't remove it and make the standarize_name transformation")
        
    @Helpers.check_airflow_task_args
    @staticmethod
    def calculate_reynolds_number(total_mass_flow: pd.Series, pipe_diameter: float, oil_vicosity:pd.Series ):

        reynolds_number = (4 * total_mass_flow)/(math.pi * pipe_diameter * oil_vicosity) 
        reynolds_number = reynolds_number.apply(lambda x : x if x > 100 else 100)
        return reynolds_number

    @Helpers.check_airflow_task_args
    @staticmethod
    def calculate_alfa_reynolds_number(roughness, reynolds_number, pipe_diameter):
        alpha_reynolds = np.log((roughness/(3.7*pipe_diameter))+(5.74/ reynolds_number ** 0.9))
        return alpha_reynolds

    @Helpers.check_airflow_task_args
    @staticmethod
    def get_pipe_roughness_by_position(parameters_pipe, position):
        length_start_aux = -1
        pipe_roughness_aux = -1

        for parameters in parameters_pipe:
            length_start = parameters['length_start']
            pipe_roughness = parameters['pipe_roughness']
            if(length_start_aux != -1):
                if(length_start_aux<=position<length_start):
                    
                    pipe_roughness = pipe_roughness_aux
                    return pipe_roughness
                        
            length_start_aux = length_start
            pipe_roughness_aux = pipe_roughness

    @Helpers.check_airflow_task_args
    @staticmethod
    def get_pipe_diameter_by_position(parameters_pipe, position):
        length_start_aux = -1
        pipe_diameter_aux = -1

        for parameters in parameters_pipe:
            length_start = parameters['length_start']
            pipe_diameter = parameters['pipe_diameter']
            if(length_start_aux != -1):
                if(length_start_aux<=position<length_start):
                    
                    pipe_diameter = pipe_diameter_aux
                    return pipe_diameter
                        
            length_start_aux = length_start
            pipe_diameter_aux = pipe_diameter

    @Helpers.check_airflow_task_args
    @staticmethod
    def get_pipe_diameters(genkey):


        for i in genkey['Network Component']:
            if 'NETWORKCOMPONENT' in i:
                if 'TYPE' in i['NETWORKCOMPONENT']:
                    if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):   
                        parameters_pipe = list()
                        pipe_length = 0
                        for j in i['PIPE']:
                            parameters_pipe.append({'length_start':pipe_length, 'pipe_diameter': j['DIAMETER']['VALUE'][0], 'pipe_roughness': j['ROUGHNESS']['VALUE'][0]})
                            pipe_length = pipe_length + j['LENGTH']['VALUE'][0]

                        return parameters_pipe

        raise ValueError("There isn't any Network Component with FLOWPATH type.")

    @Helpers.check_airflow_task_args
    @staticmethod
    def _get_tranfer_positions(df_columns: list):

        positions = [
                        int(column.split('@')[-1].split('M')[0])
                        for column in df_columns
                        if 'POS@' in column
                    ]

        positions = list(set(positions))
        positions.sort()

        return positions  
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def keep_columns_with_positions(df:pd.DataFrame, keep_positions: list):
        """
        Keep columns in a DataFrame based on specified positions.

        Args:
            df (pd.DataFrame): The input DataFrame.
            keep_positions (list): A list of positions to keep.

        Returns:
            pd.DataFrame: The DataFrame with columns filtered based on positions.
        """
        df_columns = df.columns.to_list()
        positions = Transform._get_tranfer_positions(df_columns)

        if not keep_positions:
            raise ValueError('The list of positions you provided should have at least one element.')

        for position in keep_positions:
            if position not in positions:
                raise ValueError(f'Position {position} is not in the positions of the dataframe columns.')
        
        remove_column_positions = [position for position in positions if position not in keep_positions]

        remove_columns = [column for position in remove_column_positions for column in df_columns if f"@{position}" in column]
        df_columns_dropped = Transform.drop(df, columns=remove_columns)

        return df_columns_dropped

    @Helpers.check_airflow_task_args
    @staticmethod
    def calculate_friction_factor(df: pd.DataFrame, genkey: dict ):

        df_columns = df.columns.to_list()

        positions = Transform._get_tranfer_positions(df_columns)

        for position in positions: 

            if (f"VISHLTAB_POSITION_POS@{position}M_Oil_viscosity_from_fluid_tables_N-S/M2" not in df_columns):
                raise ValueError(f'Oil viscosity from fluid column is not in the DF for the position {position} meters.')

            if (f"GT_POSITION_POS@{position}M_Total_mass_flow_KG/S" not in df_columns):
                raise ValueError(f'Total mass flow column is not in the DF for the position {position} meters.')

            oil_vicosity = df[f"VISHLTAB_POSITION_POS@{position}M_Oil_viscosity_from_fluid_tables_N-S/M2"]
            total_mass_flow = df[f"GT_POSITION_POS@{position}M_Total_mass_flow_KG/S"]

            parameters_pipe = Transform.get_pipe_diameters(genkey)
            pipe_diameter = Transform.get_pipe_diameter_by_position(parameters_pipe, position)
            roughness = Transform.get_pipe_roughness_by_position(parameters_pipe, position)

            reynolds_number = Transform.calculate_reynolds_number(total_mass_flow, pipe_diameter, oil_vicosity)
            # df[f'REYNOLDS_NUMBER_POS@{position}'] = reynolds_number
            alfa_reynolds = Transform.calculate_alfa_reynolds_number(roughness,reynolds_number,pipe_diameter)
            # df[f'ALFA_REYNOLDS_NUMBER_POS@{position}'] = alfa_reynolds
            friction_factor = ( ( (64/reynolds_number)**8) + (9.5*( (alfa_reynolds - ((2500/reynolds_number)**6) )** -16)))**(1/8)
            # return df.head()
            df[f'FRICTION_FACTOR_POS@{position}'] = friction_factor
        # alfa_reynolds = 100
        return df