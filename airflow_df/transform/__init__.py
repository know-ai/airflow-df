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
    def _concatenate_three_parts(input_string)->str:
        """
        Concatenate and format three parts of a column name from a TPL.

        This function takes an input string that represents a column name in a specific format,
        typically used in data processing pipelines. It extracts and concatenates three parts
        from the input string to create a standardized column name. The three parts typically
        include a label name, a position specifier, and a variable name, with an optional
        measurement unit enclosed in parentheses.

        The function transforms the input string format from:
        'LABEL_NAME_POS:' 'POS-XXM' 'VARIABLE_NAME_MEASURE_UNIT'
        to a standardized format:
        'LABEL_NAME_POS@XXM_VARIABLE_NAME_MEASURE_UNIT'

        If the input string does not contain a variable name or a measurement unit, those parts
        are omitted from the standardized format.

        Args:
            input_string (str): The input string representing the column name.

        Returns:
            str: The standardized and concatenated column name.

        Example:
            >>> input_string = "SSP 'POSITION:' 'POS-19M' '(M/S)' 'Speed of sound in fluid'"
            >>> result = concatenate_three_parts(input_string)
            >>> result
            'SSP_POSITION_POS@19M_Speed_of_sound_in_fluid_M/S'

        Note:
            - The function replaces spaces with underscores and converts hyphens to '@' for position specifiers.
            - The measurement unit, if present, is appended to the standardized name.
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
        """
        Standardize column names in a DataFrame.

        This function standardizes the column names of a given DataFrame by applying the
        'Transform.concatenate_three_parts' function to each column name. It renames the
        columns using the standardized names and returns the updated DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame whose column names need standardization.
            *args: Additional positional arguments (ignored).
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            pd.DataFrame | None: The DataFrame with standardized column names or None if no changes were made.

        Example:
            >>> import pandas as pd
            >>> data = {
            ...     "SSP 'POSITION:' 'POS-19M' '(M/S)' 'Speed of sound in fluid'": [1, 2, 3],
            ...     "Another_Column": [4, 5, 6]
            ... }
            >>> df = pd.DataFrame(data)
            >>> result = standarize_column_names(df)
            >>> result.columns
            Index(['SSP_POSITION_POS@19M_Speed_of_sound_in_fluid_M/S', 'Another_Column'], dtype='object')

        Note:
            - The function relies on 'Transform.concatenate_three_parts' for standardization.
            - Any additional positional or keyword arguments passed to the function are ignored.
        """
        column_names = df.columns.to_list()
        column_standarize_names = dict()
        for i in column_names:
            column_standarize_names[i] = Transform._concatenate_three_parts(i)
        df = Transform.rename(df, columns=column_standarize_names)
        return df
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def _get_leak_size_from_genkey(genkey: dict) -> float | None:
        """
        Extracts the leak size from a given genkey dictionary.

        Args:
            genkey (dict): The genkey dictionary containing network component information.

        Returns:
            float | None: The size of the leak, or None if no leak is found.

        """
        for i in genkey['Network Component']:
                        if 'NETWORKCOMPONENT' in i:
                            if 'TYPE' in i['NETWORKCOMPONENT']:
                                if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):   
                                    if('LEAK' in i):
                                        leak_size =  i['LEAK']['DIAMETER']['VALUE'][0]
                                        return leak_size
        return None

    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_size(df: pd.DataFrame, genkey: dict, constant_leak_size:bool = False, control_leak_column:str = 'CONTR_CONTROLLER_CONTROL_LEAK_Controller_output', manually: bool = False, leak_size: float | None = None)->pd.DataFrame:
        """
            Add leak size information to a DataFrame based on the provided genkey.

            This function optionally searches for information about a leak size in the provided 'genkey' dictionary,
            which contains parameters related to network components, but it performs the search only when 'manually' is set to False.
            If it finds a leak size associated with a network component of type 'FLOWPATH', it adds this leak size information as a new
            column 'LEAK_SIZE' to the DataFrame.

            Args:
                df (pd.DataFrame): The DataFrame to which the leak size information will be added.
                genkey (dict): The dictionary containing network component information.
                constant_leak_size (bool): If True, the 'LEAK_SIZE' column will have a constant value regardless of the control leak column.
                    If False, the 'LEAK_SIZE' column will be multiplied by the control leak column values. Default is False.
                control_leak_column (str): The name of the control leak column in the DataFrame when 'constant_leak_size' is False.
                    Default is 'CONTR_CONTROLLER_CONTROL_LEAK_Controller_output'.
                manually (bool): If True, the leak size is provided manually, and 'leak_size' must be specified.
                    If False, the function searches for the leak size in the 'genkey' dictionary.
                leak_size (float | None): The leak size to be added to the DataFrame when 'manually' is True.
                    Default is None.

            Returns:
                pd.DataFrame: The DataFrame with the 'LEAK_SIZE' column added.

            Raises:
                ValueError: If there is no leak size information found in the 'genkey' simulation or if 'leak_size' is invalid.

            Example:
                >>> import pandas as pd
                >>> genkey = {
                ...     'Network Component': [
                ...         {
                ...             'NETWORKCOMPONENT': {
                ...                 'TYPE': 'FLOWPATH'
                ...             },
                ...             'LEAK': {
                ...                 'DIAMETER': {'VALUE': [0.005]}
                ...             }
                ...         }
                ...     ]
                ... }
                >>> data = {
                ...     'Other_Column': [1, 2, 3],
                ...     'CONTR_CONTROLLER_CONTROL_LEAK_Controller_output': [
                ...         0.000000, 0.061748, 0.546996, 1.000000, 1.000000, 0.455432, 0.000000, 0.000000, 0.000000
                ...     ]
                ... }
                >>> df = pd.DataFrame(data)
                >>> df = add_leak_size(df, genkey)
                >>> df
                Other_Column  LEAK_SIZE
                0             1    0.000000
                1             2    0.061748
                2             3    0.546996

            Note:
                - The function searches for leak size information in the 'genkey' dictionary when 'manually' is set to False.
                - If 'manually' is True, 'leak_size' must be provided.
                - If 'constant_leak_size' is True, the 'LEAK_SIZE' column will have a constant value.
                - If 'constant_leak_size' is False, the 'LEAK_SIZE' column will be multiplied by the control leak column values.
                - It raises a ValueError if no leak size information is found or if 'leak_size' is invalid.
        """

        if(not manually):
            leak_size =  Transform._get_leak_size_from_genkey(genkey)

            if(leak_size is None):
                raise ValueError('There is no leak size in this genkey simulation.')
            
            if(constant_leak_size):    
                df['LEAK_SIZE'] = leak_size
            else:
                df['LEAK_SIZE'] = leak_size*df[control_leak_column]

            return df
        else:
            if(leak_size is None):
                raise ValueError('leak_size cannot be a None value.')
            if(leak_size < 0):
                raise ValueError('leak_size has to be a positive integer.')
            
            if(constant_leak_size):    
                df['LEAK_SIZE'] = leak_size
            else:
                df['LEAK_SIZE'] = leak_size*df[control_leak_column]

            return df
    

    @Helpers.check_airflow_task_args
    @staticmethod
    def _get_leak_location_from_genkey(genkey: dict) -> int | None:
        """
        Extracts the leak location from a given genkey dictionary.

        Args:
            genkey (dict): The genkey dictionary containing network component information.

        Returns:
            int | None: The location of the leak, or None if no leak is found.

        """
        for i in genkey['Network Component']:
            if 'NETWORKCOMPONENT' in i:
                if 'TYPE' in i['NETWORKCOMPONENT']:
                    if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):   
                        if('LEAK' in i):

                            leak_location =  i['LEAK']['ABSPOSITION']['VALUE'][0]
                            return leak_location
        return None
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_location(df:pd.DataFrame, genkey: dict, manually: bool = False, leak_location: float | None = None)->pd.DataFrame:
        """
        Add leak location information to a DataFrame based on the provided genkey.

        This function optionally searches for information about a leak location in the provided 'genkey' dictionary,
        which contains parameters related to network components. It performs the search only when 'manually' is set to False.
        If it finds a leak location associated with a network component of type 'FLOWPATH', it adds this leak location information
        as a new column 'LEAK_LOCATION' to the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to which the leak location information will be added.
            genkey (dict): The dictionary containing network component information.
            manually (bool): If True, the leak location is provided manually, and 'leak_location' must be specified.
                If False, the function searches for the leak location in the 'genkey' dictionary. Default is False.
            leak_location (float | None): The leak location to be added to the DataFrame when 'manually' is True.
                Default is None.

        Returns:
            pd.DataFrame: The DataFrame with the 'LEAK_LOCATION' column added.

        Raises:
            ValueError: If there is no leak location information found in the 'genkey' simulation or if 'leak_location' is invalid.

        Example:
            >>> import pandas as pd
            >>> genkey = {
            ...     'Network Component': [
            ...         {
            ...             'NETWORKCOMPONENT': {
            ...                 'TYPE': 'FLOWPATH'
            ...             },
            ...             'LEAK': {
            ...                 'ABSPOSITION': {'VALUE': [10.5]}
            ...             }
            ...         }
            ...     ]
            ... }
            >>> data = {
            ...     'Other_Column': [1, 2, 3]
            ... }
            >>> df = pd.DataFrame(data)
            >>> df = add_leak_location(df, genkey)
            >>> df
            Other_Column  LEAK_LOCATION
            0             1           10.5
            1             2           10.5
            2             3           10.5

        Note:
            - The function searches for leak location information in the 'genkey' dictionary when 'manually' is set to False.
            - If 'manually' is True, 'leak_location' must be provided.
            - It raises a ValueError if no leak location information is found or if 'leak_location' is invalid.
        """
        if(not manually):
            
            leak_location =  Transform._get_leak_location_from_genkey(genkey)

            if(leak_location is None):
                raise ValueError('There is no leak location in this genkey simulation.')

            df['LEAK_LOCATION'] = leak_location

            return df
        else:

            if(leak_location is None):
                raise ValueError('leak_location cannot be a None value.')
            if(leak_location < 0):
                raise ValueError('leak_location has to be a positive integer.')

            df['LEAK_LOCATION'] = leak_location
            
            return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def add_leak_status(df: pd.DataFrame, genkey: dict, manually: bool = False, time_values: list | None = None, leak_sign: list | None = None, time_series_column: str = 'TIME_SERIES_S') -> pd.DataFrame:
        """
        Add leak status information to a DataFrame based on the provided genkey.

        This function determines the leak status for each time point in the DataFrame 'df' based on the
        information provided in the 'genkey' dictionary. If the DataFrame contains a 'TIME_SERIES_S' column,
        and the 'genkey' includes information about a 'Control-Leak' with setpoints and time values, it adds
        a 'LEAK_STATUS' column to the DataFrame. The 'LEAK_STATUS' column indicates whether there is a leak
        at each time point ('LEAK') or not ('NO LEAK').

        Args:
            df (pd.DataFrame): The DataFrame to which the leak status information will be added.
            genkey (dict): The dictionary containing network component information, including leak control data.

        Returns:
            pd.DataFrame: The DataFrame with the 'LEAK_STATUS' column added.

        Raises:
            KeyError: If the 'TIME_SERIES_S' column is not present in the DataFrame.

        Example:
            >>> import pandas as pd
            >>> genkey = {
            ...     'Network Component': [
            ...         {
            ...             'PARAMETERS': {
            ...                 'LABEL': 'Control-Leak',
            ...                 'SETPOINT': [0, 1],
            ...                 'TIME': {'VALUE': [0, 10]}
            ...             }
            ...         }
            ...     ]
            ... }
            >>> data = {
            ...     'TIME_SERIES_S': [2, 5, 8, 15]
            ... }
            >>> df = pd.DataFrame(data)
            >>> df = add_leak_status(df, genkey)
            >>> df
            TIME_SERIES_S LEAK_STATUS
            0              2      NO LEAK
            1              5        LEAK
            2              8        LEAK
            3             15      NO LEAK

        Note:
            - The function looks for 'TIME_SERIES_S' in the DataFrame and uses it for time comparisons.
            - The 'genkey' dictionary should include information about 'Control-Leak' with setpoints and time values.
            - It raises a KeyError if 'TIME_SERIES_S' is not present in the DataFrame.
        """
        if(manually):
            if(time_series_column in df):
                leak_ranges = list()
                for i in range(len(leak_sign)):
                    if(leak_sign[i]):
                        if(i+1 < len(time_values)):
                            leak_ranges.append((time_values[i], time_values[i+1]))
                        else:
                            leak_ranges.append((time_values[i], float('inf')))
                df['LEAK_STATUS'] = 'NO LEAK'
                for i in leak_ranges:
                    df.loc[(i[0]<df[time_series_column])&(df[time_series_column]<i[1]), 'LEAK_STATUS'] = 'LEAK'
            else:
                raise KeyError(f"You must have the column {time_series_column} to do this transformation.\nPlease, don't remove it and make the standarize_name transformation")
        
        else:
            if(time_series_column in df):
                for i in genkey['Network Component']:
                    if 'LABEL' in i['PARAMETERS']:
                        if('Control-Leak' == i['PARAMETERS']['LABEL']):   
                            leak_sign = i['PARAMETERS']['SETPOINT']
                            time_values = i['PARAMETERS']['TIME']['VALUE']
                            break
                if(type(leak_sign)==int):
                    df['LEAK_STATUS'] = 'NO LEAK'

                elif(type(leak_sign)==list):
                    leak_ranges = list()
                    for i in range(len(leak_sign)):
                        if(leak_sign[i]):
                            if(i+1 < len(time_values)):
                                leak_ranges.append((time_values[i], time_values[i+1]))
                            else:
                                leak_ranges.append((time_values[i], float('inf')))
                    df['LEAK_STATUS'] = 'NO LEAK'
                    for i in leak_ranges:
                        df.loc[(i[0]<df[time_series_column])&(df[time_series_column]<i[1]), 'LEAK_STATUS'] = 'LEAK'
                
                return df 
            else:
                raise KeyError(f"You must have the column {time_series_column} to do this transformation.\nPlease, don't remove it and make the standarize_name transformation")
        
    @Helpers.check_airflow_task_args
    @staticmethod
    def calculate_reynolds_number(total_mass_flow: pd.Series, pipe_diameter: float, oil_vicosity:pd.Series ):
        """
            Calculate the Reynolds number for fluid flow in a pipe.

            This function computes the Reynolds number for fluid flow in a pipe using the given total mass flow,
            pipe diameter, and oil viscosity. The Reynolds number is a dimensionless quantity used to predict
            the flow regime (e.g., laminar or turbulent) in fluid dynamics.

            Args:
                total_mass_flow (pd.Series): A pandas Series containing total mass flow values.
                pipe_diameter (float): The diameter of the pipe (in meters).
                oil_viscosity (pd.Series): A pandas Series containing oil viscosity values.

            Returns:
                pd.Series: A pandas Series containing the calculated Reynolds numbers.

            Example:
                >>> total_mass_flow = pd.Series([100, 200, 150])
                >>> pipe_diameter = 0.1
                >>> oil_viscosity = pd.Series([0.01, 0.02, 0.015])
                >>> reynolds_numbers = calculate_reynolds_number(total_mass_flow, pipe_diameter, oil_viscosity)
                >>> reynolds_numbers
                0    400000.0
                1    200000.0
                2    266666.67
                dtype: float64
        """
        reynolds_number = (4 * total_mass_flow)/(math.pi * pipe_diameter * oil_vicosity) 
        reynolds_number = reynolds_number.apply(lambda x : x if x > 100 else 100)
        return reynolds_number

    @Helpers.check_airflow_task_args
    @staticmethod
    def calculate_alfa_reynolds_number(roughness: float, reynolds_number: pd.Series, pipe_diameter: float):
        """
        Calculate the alpha-Reynolds number for fluid flow in a pipe.

        This function computes the alpha-Reynolds number for fluid flow in a pipe using the given pipe roughness,
        Reynolds number, and pipe diameter. The alpha-Reynolds number is used to characterize the pipe flow
        and is related to the pipe roughness and Reynolds number.

        Args:
            roughness (float): The roughness of the pipe (in meters).
            reynolds_number (pd.Series): A pandas Series containing Reynolds number values.
            pipe_diameter (float): The diameter of the pipe (in meters).

        Returns:
            pd.Series: A pandas Series containing the calculated alpha-Reynolds numbers.

        Example:
            >>> roughness = 0.005
            >>> reynolds_numbers = pd.Series([100000, 200000, 150000])
            >>> pipe_diameter = 0.1
            >>> alpha_reynolds_numbers = calculate_alfa_reynolds_number(roughness, reynolds_numbers, pipe_diameter)
            >>> alpha_reynolds_numbers
            0    3.574334
            1    3.405992
            2    3.501679
            dtype: float64
        """    
        
        alpha_reynolds = np.log((roughness/(3.7*pipe_diameter))+(5.74/ reynolds_number ** 0.9))
        return alpha_reynolds

    @Helpers.check_airflow_task_args
    @staticmethod
    def get_pipe_roughness_by_position(parameters_pipe, position):
        """
        Get the pipe roughness at a specific position along a pipe based on provided parameters.

        This function calculates the pipe roughness at a given position along a pipe based on a list of
        pipe parameters and the specified position. It iterates through the provided parameters to find
        the appropriate pipe roughness corresponding to the position.

        Args:
            parameters_pipe (list): A list of dictionaries, each containing 'length_start' and 'pipe_roughness'.
                These parameters define the pipe roughness changes along the pipe.
            position (float): The position along the pipe for which to retrieve the pipe roughness.

        Returns:
            float: The pipe roughness at the specified position.

        Example:
            >>> parameters_pipe = [
            ...     {'length_start': 0, 'pipe_roughness': 0.005},
            ...     {'length_start': 100, 'pipe_roughness': 0.006},
            ...     {'length_start': 200, 'pipe_roughness': 0.004}
            ... ]
            >>> position = 150
            >>> roughness = get_pipe_roughness_by_position(parameters_pipe, position)
            >>> roughness
            0.006
        Note:
            You have had ran the function get_pipe_diameters to pass the argument parameters_pipe.
        """
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

        """
        Get the pipe diameter at a specific position along a pipe based on provided parameters.

        This function calculates the pipe diameter at a given position along a pipe based on a list of
        pipe parameters and the specified position. It iterates through the provided parameters to find
        the appropriate pipe diameter corresponding to the position.

        Args:
            parameters_pipe (list): A list of dictionaries, each containing 'length_start' and 'pipe_diameter'.
                These parameters define the pipe diameter changes along the pipe.
            position (float): The position along the pipe for which to retrieve the pipe diameter.

        Returns:
            float: The pipe diameter at the specified position.

        Example:
            >>> parameters_pipe = [
            ...     {'length_start': 0, 'pipe_diameter': 0.2},
            ...     {'length_start': 100, 'pipe_diameter': 0.3},
            ...     {'length_start': 200, 'pipe_diameter': 0.25}
            ... ]
            >>> position = 150
            >>> diameter = get_pipe_diameter_by_position(parameters_pipe, position)
            >>> diameter
            0.3
        Note:
            You have had ran the function get_pipe_diameters to pass the argument parameters_pipe.
        """
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
        """
        Get pipe diameter and roughness parameters from the given genkey dictionary.

        This function extracts pipe diameter and roughness information from the 'genkey' dictionary,
        which contains information about network components. It specifically looks for network components
        of type 'FLOWPATH' and extracts relevant data.

        Args:
            genkey (dict): The dictionary containing network component information.

        Returns:
            list: A list of dictionaries, each containing 'length_start', 'pipe_diameter', and 'pipe_roughness'.
                This information is extracted from the 'genkey' dictionary for the FLOWPATH network component.

        Raises:
            ValueError: If there are no network components of type 'FLOWPATH' in the 'genkey' dictionary.

        Example:
            >>> genkey = {
            ...     'Network Component': [
            ...         {
            ...             'NETWORKCOMPONENT': {
            ...                 'TYPE': 'FLOWPATH'
            ...             },
            ...             'PIPE': [
            ...                 {
            ...                     'DIAMETER': {'VALUE': [0.2]},
            ...                     'ROUGHNESS': {'VALUE': [0.005]},
            ...                     'LENGTH': {'VALUE': [100]}
            ...                 },
            ...                 {
            ...                     'DIAMETER': {'VALUE': [0.3]},
            ...                     'ROUGHNESS': {'VALUE': [0.006]},
            ...                     'LENGTH': {'VALUE': [50]}
            ...                 }
            ...             ]
            ...         }
            ...     ]
            ... }
            >>> pipe_parameters = get_pipe_diameters(genkey)
            >>> pipe_parameters
            [{'length_start': 0, 'pipe_diameter': 0.2, 'pipe_roughness': 0.005},
            {'length_start': 100, 'pipe_diameter': 0.3, 'pipe_roughness': 0.006}]
        Note:
            This is mainly used to get the friction factor getting 
        """

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
    def _get_tranfer_positions_tpl(df_columns: list):
        """
            Retrieve and return a sorted list of transfer positions from a list of DataFrame columns.

            This function parses column names to extract transfer positions marked with 'POS@'.
            It then removes duplicates, sorts the positions in ascending order, and returns the result.

            Args:
                df_columns (list): A list of column names to extract transfer positions from.

            Returns:
                list: A sorted list of unique transfer positions found in the column names.

            Example:
                >>> columns = ['ROHL_POSITION_POS@19M_Oil_density_KG/M3', 'SSP_POSITION_POS@19M_Speed_of_sound_in_fluid_M/S', 'GT_POSITION_POS@50M_Total_mass_flow_KG/S', 'SSP_POSITION_POS@660M_Speed_of_sound_in_fluid_M/S', 'Goals@POS@3M']
                >>> _get_transfer_positions(columns)
                [19, 50, 660]

            Note:
                This function assumes that transfer positions are marked in column names with 'POS@'
        """
        positions = [
                        int(column.split('@')[-1].split('M')[0])
                        for column in df_columns
                        if '@' in column
                    ]

        positions = list(set(positions))
        positions.sort()

        return positions
    
    @Helpers.check_airflow_task_args
    @staticmethod
    def _extract_pipeline_name(column_name: str) -> str | None:
        """
            Extracts the pipeline name from a given column name string.

            Args:
                column_name (str): The input column name string from which the pipeline name will be extracted.

            Returns:
                str | None: If a pipeline name is found in the column name string, it returns the extracted pipeline name as a string.
                            If no pipeline name is found, it returns None.
        """
        pattern = r'_([A-Z0-9]+)@'
        matches = re.findall(pattern, column_name)
        if matches:
            return matches[0]
        else:
            return None
        
    @Helpers.check_airflow_task_args
    @staticmethod
    def _get_unique_pipelines_name_tpl(df_columns: list) -> list:
        """
            Returns a list of unique pipeline names extracted from a list of column names, removing any None values.

            Args:
                df_columns (list): A list of column names from which the unique pipeline names will be extracted.

            Returns:
                list: A list of unique pipeline names (strings) extracted from the column names, with None values removed.
        """
        pipeline_names = [Transform._extract_pipeline_name(string) for string in df_columns]
        pipeline_names = [x for x in pipeline_names if x is not None]
        pipeline_names = list(set(pipeline_names))
        return pipeline_names
    


    @Helpers.check_airflow_task_args
    @staticmethod
    def _get_tranfer_positions_genkey(genkey: dict)->list | None:
        for i in genkey['Network Component']:
            if('FLOWPATH' == i['NETWORKCOMPONENT']['TYPE']):
                for j in i['TRENDDATA']:
                    if('ABSPOSITION' in j):
                        return j['ABSPOSITION']['VALUE']
                        
        return None
    
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
        positions = Transform._get_tranfer_positions_tpl(df_columns)

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
    def calculate_friction_factor(df: pd.DataFrame, genkey: dict, add_reynolds_number: bool = False, add_alfa_reynolds_number: bool = False ):
        
        """
        Calculate the friction factor for fluid flow in pipes at different positions.

        This function computes the friction factor for fluid flow in pipes at specified positions based on
        the given DataFrame and additional parameters. It can optionally add the Reynolds number and
        alpha-Reynolds number to the DataFrame for each position.

        Args:
            df (pd.DataFrame): The DataFrame containing relevant data for fluid flow analysis.
            genkey (dict): A dictionary containing general parameters for the analysis.
            add_reynolds_number (bool, optional): Whether to add the Reynolds number to the DataFrame.
                Defaults to False.
            add_alfa_reynolds_number (bool, optional): Whether to add the alpha-Reynolds number to the DataFrame.
                Defaults to False.

        Returns:
            pd.DataFrame: The DataFrame with computed friction factors and optionally added Reynolds and
                alpha-Reynolds numbers.

        Raises:
            ValueError: If required columns for oil viscosity and total mass flow are not present in the DataFrame.

        Example:
            >>> import pandas as pd
            >>> data = {
            ...     'VISHLTAB_POSITION_POS@1M_Oil_viscosity_from_fluid_tables_N-S/M2': [0.01, 0.02],
            ...     'GT_POSITION_POS@1M_Total_mass_flow_KG/S': [100, 200],
            ...     # Add other required columns
            ... }
            >>> df = pd.DataFrame(data)
            >>> genkey = {'other_parameters': 'values'}
            >>> df = Transform.calculate_friction_factor(df, genkey, add_reynolds_number=True, add_alfa_reynolds_number=True)
        
        """
        
        df_columns = df.columns.to_list()

        positions = Transform._get_tranfer_positions_tpl(df_columns)
        unique_pipeline_names = Transform._get_unique_pipelines_name_tpl(df_columns)
        pipeline_name = unique_pipeline_names[0]

        for position in positions: 

            if (f"VISHLTAB_POSITION_{pipeline_name}@{position}M_Oil_viscosity_from_fluid_tables_N-S/M2" not in df_columns):
                raise ValueError(f'Oil viscosity from fluid column is not in the DF for the position {position} meters.')

            if (f"GT_POSITION_{pipeline_name}@{position}M_Total_mass_flow_KG/S" not in df_columns):
                raise ValueError(f'Total mass flow column is not in the DF for the position {position} meters.')

            oil_vicosity = df[f"VISHLTAB_POSITION_{pipeline_name}@{position}M_Oil_viscosity_from_fluid_tables_N-S/M2"]
            total_mass_flow = df[f"GT_POSITION_{pipeline_name}@{position}M_Total_mass_flow_KG/S"]

            parameters_pipe = Transform.get_pipe_diameters(genkey)
            pipe_diameter = Transform.get_pipe_diameter_by_position(parameters_pipe, position)
            roughness = Transform.get_pipe_roughness_by_position(parameters_pipe, position)

            reynolds_number = Transform.calculate_reynolds_number(total_mass_flow, pipe_diameter, oil_vicosity)
            if(add_reynolds_number):
                df[f'REYNOLDS_NUMBER_{pipeline_name}@{position}'] = reynolds_number
            
            alfa_reynolds = Transform.calculate_alfa_reynolds_number(roughness,reynolds_number,pipe_diameter)
            if(add_alfa_reynolds_number):
                df[f'ALFA_REYNOLDS_NUMBER_{pipeline_name}@{position}'] = alfa_reynolds

            friction_factor = ( ( (64/reynolds_number)**8) + (9.5*( (alfa_reynolds - ((2500/reynolds_number)**6) )** -16)))**(1/8)
            df[f'FRICTION_FACTOR_{pipeline_name}@{position}'] = friction_factor

        return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def convert_mass_fluid_barrel_per_hour_to_KG_per_second(df: pd.DataFrame, density_columns: list | None = None, mass_flow_columns: list | None = None) -> pd.DataFrame:
        """
            Convert mass flow rates from fluid barrels per hour to kilograms per second in a DataFrame.

            This function performs the conversion of mass flow rates in a DataFrame from units of fluid barrels per hour
            to units of kilograms per second. It requires specifying the columns containing mass flow rates and the
            corresponding columns containing fluid density values. The conversion formula used is based on the
            relationship between fluid barrels and kilograms.

            Args:
                df (pd.DataFrame): The DataFrame containing mass flow rates and density values.
                density_columns (list | None): A list of column names containing fluid density values in kg/m³.
                    If None, the function will attempt to identify density columns from the DataFrame by looking into the positions of the transfer.
                mass_flow_columns (list | None): A list of column names containing mass flow rates in barrels per hour.
                    If None, the function will attempt to identify mass flow columns from the DataFrame by looking into the positions of the transfer.

            Returns:
                pd.DataFrame: The DataFrame with mass flow rates converted to kilograms per second.

            Raises:
                ValueError: If the number of density columns and mass flow columns do not match.

            Example:
                >>> import pandas as pd
                >>> data = {
                ...     'GT_POSITION_POS@10M_Total_mass_flow_KG/S': [100, 200, 150],
                ...     'ROHL_POSITION_POS@10M_Oil_density_KG/M3': [800, 900, 750],
                ... }
                >>> df = pd.DataFrame(data)
                >>> df = convert_mass_fluid_barrel_per_hour_to_KG_per_second(df)
                >>> df
                GT_POSITION_POS@10M_Total_mass_flow_KG/S  ROHL_POSITION_POS@10M_Oil_density_KG/M3
                0                                      3.530374                                  800
                1                                      7.060748                                  900
                2                                      5.295561                                  750

            Note:
                - The conversion factor used is 0.158787, based on the relationship between fluid barrels and kilograms.
                - The function identifies density and mass flow columns from the DataFrame if they are not provided.
                - It raises a ValueError if the number of density columns and mass flow columns does not match.
        """        

        positions = list()
        df_columns = list()
        
        if(mass_flow_columns is None or density_columns is None):
            positions = Transform._get_tranfer_positions_tpl(df.columns.to_list())
            df_columns = df.columns.to_list()

        if(mass_flow_columns is None):
            mass_flow_columns = list()

            for position in positions: 
                if (f"GT_POSITION_POS@{position}M_Total_mass_flow_KG/S" not in df_columns):
                    raise ValueError(f'Total mass flow column is not in the DF for the position {position} meters.')
                mass_flow_columns.append(f"GT_POSITION_POS@{position}M_Total_mass_flow_KG/S")
        
        if(density_columns is None):
            density_columns = list()

            for position in positions: 
                if (f"ROHL_POSITION_POS@{position}M_Oil_density_KG/M3" not in df_columns):
                    raise ValueError(f'Total mass flow column is not in the DF for the position {position} meters.')
                density_columns.append(f"ROHL_POSITION_POS@{position}M_Oil_density_KG/M3")


        if(len(mass_flow_columns) != len(density_columns)):
            raise ValueError('You should pass the same number of columns for density and mass flow.')
        for i in range(len(mass_flow_columns)):

            df[mass_flow_columns[i]] = df[mass_flow_columns[i]] * 0.158787 * df[density_columns[i]]/3600

        return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def convert_pressure_psig_to_pascal(df: pd.DataFrame, pressure_columns: list | None = None)-> pd.DataFrame:
        """
        Convert pressure values from psig (pounds per square inch gauge) to pascals in a DataFrame.

        This function performs the conversion of pressure values in a DataFrame from units of pounds per square inch
        gauge (psig) to units of pascals (Pa). It requires specifying the columns containing pressure values.
        The conversion factor used is 6894.75728, which is the approximate conversion factor from psig to pascals.

        Args:
            df (pd.DataFrame): The DataFrame containing pressure values to be converted.
            pressure_columns (list | None): A list of column names containing pressure values in psig.
                If None, the function will attempt to identify pressure columns from the DataFrame by looking into the positions of the transfer.

        Returns:
            pd.DataFrame: The DataFrame with pressure values converted to pascals.

        Raises:
            ValueError: If the pressure columns are not provided, or if a pressure column is not found for a position.

        Example:
            >>> import pandas as pd
            >>> data = {
            ...     'PT_POSITION_POS@10M_Pressure_PA': [1000, 2000, 1500],
            ... }
            >>> df = pd.DataFrame(data)
            >>> df = convert_pressure_psig_to_pascal(df)
            >>> df
            PT_POSITION_POS@10M_Pressure_PA
            0                             6894757.28
            1                            13789514.56
            2                            10342135.92

        Note:
            - The conversion factor used is 6894.75728, which is an approximate factor for psig to pascals conversion.
            - The function identifies pressure columns from the DataFrame if they are not provided.
            - It raises a ValueError if pressure columns are not provided or if a pressure column is not found.
        """
        positions = list()
        df_columns = list()
        
        if(pressure_columns is None):
            positions = Transform._get_tranfer_positions_tpl(df.columns.to_list())
            df_columns = df.columns.to_list()


        if(pressure_columns is None):
            pressure_columns = list()

            for position in positions: 

                if (f"PT_POSITION_POS@{position}M_Pressure_PA" not in df_columns):
                    raise ValueError(f'Pressure column is not in the DF for the position {position} meters.')
                pressure_columns.append(f"PT_POSITION_POS@{position}M_Pressure_PA")
        
        for i in range(len(pressure_columns)):

            df[pressure_columns[i]] = df[pressure_columns[i]] * 6894.75728
        
        return df

    @Helpers.check_airflow_task_args
    @staticmethod
    def convert_temperature_fahrenheit_to_celsius(df: pd.DataFrame, temperature_columns: list | None = None)->pd.DataFrame:
        """
            Convert temperature values from Fahrenheit to Celsius in a DataFrame.

            This function performs the conversion of temperature values in a DataFrame from units of Fahrenheit (°F)
            to units of Celsius (°C). It requires specifying the columns containing temperature values.
            The conversion formula used is (°F - 32) * (5/9), which is the standard formula for converting
            Fahrenheit to Celsius.

            Args:
                df (pd.DataFrame): The DataFrame containing temperature values to be converted.
                temperature_columns (list | None): A list of column names containing temperature values in °F.
                    If None, the function will attempt to identify temperature columns from the DataFrame by looking into the positions of the transfer.

            Returns:
                pd.DataFrame: The DataFrame with temperature values converted to Celsius.

            Raises:
                ValueError: If the temperature columns are not provided, or if a temperature column is not found for a position.

            Example:
                >>> import pandas as pd
                >>> data = {
                ...     'TM_POSITION_POS@10M_Fluid_temperature_C': [68, 77, 86],
                ... }
                >>> df = pd.DataFrame(data)
                >>> df = convert_temperature_fahrenheit_to_celsius(df)
                >>> df
                TM_POSITION_POS@10M_Fluid_temperature_C
                0                                 20.0
                1                                 25.0
                2                                 30.0

            Note:
                - The conversion formula used is (°F - 32) * (5/9) for Fahrenheit to Celsius conversion.
                - The function identifies temperature columns from the DataFrame if they are not provided.
                - It raises a ValueError if temperature columns are not provided or if a temperature column is not found.
        """
        positions = list()
        df_columns = list()
        
        if(temperature_columns is None):
            positions = Transform._get_tranfer_positions_tpl(df.columns.to_list())
            df_columns = df.columns.to_list()


        if(temperature_columns is None):
            temperature_columns = list()

            for position in positions: 
                if (f"TM_POSITION_POS@{position}M_Fluid_temperature_C" not in df_columns):
                    raise ValueError(f'Temperature column is not in the DF for the position {position} meters.')
                temperature_columns.append(f"TM_POSITION_POS@{position}M_Fluid_temperature_C")
        
        for i in range(len(temperature_columns)):

            df[temperature_columns[i]] = (df[temperature_columns[i]] - 32) * (5/9)
        
        return df
            
    @Helpers.check_airflow_task_args
    @staticmethod    
    def convert_timestamp_timeseries(df:pd.DataFrame, column_timestamp:str = 'timestamp', new_column_timeseries: str = 'TIME_SERIES_S')->pd.DataFrame:
        """
        Convert a timestamp column to a time series column in seconds relative to the first timestamp.

        This function takes a DataFrame with a timestamp column and converts it into a time series column
        representing time in seconds relative to the first timestamp in the dataset. The resulting time series
        is added as a new column in the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame containing the timestamp data.
            column_timestamp (str): The name of the timestamp column in the DataFrame. Default is 'timestamp'.
            new_column_timeseries (str): The name of the new time series column to be added. Default is 'TIME_SERIES_S'.

        Returns:
            pd.DataFrame: The DataFrame with the new time series column.

        Example:
            >>> import pandas as pd
            >>> data = {
            ...     'timestamp': ['2023-09-18 10:00:00.000', '2023-09-18 10:00:01.000', '2023-09-18 10:00:02.000']
            ... }
            >>> df = pd.DataFrame(data)
            >>> df = convert_timestamp_timeseries(df)
            >>> df
                        timestamp  TIME_SERIES_S
            0  2023-09-18 10:00:00.000             0.0
            1  2023-09-18 10:00:01.000             1.0
            2  2023-09-18 10:00:02.000             2.0

        Note:
            - The function assumes that the timestamp column is in the format 'YYYY-MM-DD HH:MM:SS.SSS'.
            - The new time series column represents time in seconds relative to the first timestamp in the dataset.
        """

        timestamps = pd.to_datetime(df[column_timestamp], format='%Y-%m-%d %H:%M:%S.%f')
        time_diff_from_start = (timestamps - timestamps.iloc[0]).dt.total_seconds()
        df[new_column_timeseries] = time_diff_from_start
        
        return  df
    
    @Helpers.check_airflow_task_args
    @staticmethod    
    def _check_for_gtsource_column(df: pd.DataFrame) -> bool :
        df_columns = df.columns.to_list()
        
        if('GTSOUR_SOURCE_IN_Source_mass_rate_KG/S' in df_columns):
            if(list(df['GTLEAK_LEAK_LEAK_Leakage_total_mass_flow_rate_KG/S'].unique()) == [0]):
            
                return True
            else:
                return False
        else:
            return False    

    @Helpers.check_airflow_task_args
    @staticmethod    
    def replace_gtleak_gtsource_column(df: pd.DataFrame) -> pd.DataFrame:
        have_to_replace_column = Transform._check_for_gtsource_column(df)

        if(have_to_replace_column):
            df['GTLEAK_LEAK_LEAK_Leakage_total_mass_flow_rate_KG/S_DUPLICATED'] = df['GTLEAK_LEAK_LEAK_Leakage_total_mass_flow_rate_KG/S']
            df['GTLEAK_LEAK_LEAK_Leakage_total_mass_flow_rate_KG/S'] = df['GTSOUR_SOURCE_IN_Source_mass_rate_KG/S']
            
            return df
        else:
            return df
        