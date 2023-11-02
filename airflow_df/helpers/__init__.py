import inspect
import functools
from airflow.decorators import task
import os

class Helpers:
    r"""
    This auxiliary class groups a series of static methods to help you to do some operations
    """

    @staticmethod
    def decorator(declared_decorator):
        r"""
        This static methos is used to decorate some function to make it into a decorator.

        # Snippet Code
        ```python
        from airflow_df.helpers import Helpers

        @Helpers.decorator
        def some_function_to_convert_it_into_decorator(*args, **kwargs):
            pass

        ```
        """

        @functools.wraps(declared_decorator)
        def final_decorator(func=None, **kwargs):
            # This will be exposed to the rest of your application as a decorator
            def decorated(func):
                # This will be exposed to the rest of your application as a decorated
                # function, regardless how it was called
                @functools.wraps(func)
                def wrapper(*a, **kw):
                    # This is used when actually executing the function that was decorated

                    return declared_decorator(func, a, kw, **kwargs)
                
                return wrapper
            
            if func is None:
                
                return decorated
            
            else:
                # The decorator was called without arguments, so the function should be
                # decorated immediately
                return decorated(func)

        return final_decorator

    @staticmethod
    def as_airflow_tasks():
        r"""
        This is a class decorator that allows to you decorate every method without starts with (_) in a class to make it compatible with an airflow task

        ```python
        from airflow_df.helpers import Helpers

        @Helpers.as_airflow_tasks()
        class Transform:
            
            pass
            
        ```

        Every method defined here is compatible with airflow like a task.

        """
        def decorate(cls):

            methods = inspect.getmembers(cls, predicate=lambda x: inspect.isroutine(x))

            for fn_name, fn in methods:

                if not fn_name.startswith('_'):

                    doc = fn.__doc__
                    setattr(cls, fn_name, task(fn))
                    fn = getattr(cls, fn_name)
                    fn.__doc__ = doc             
                        
            return cls

        return decorate

    @decorator
    @staticmethod
    def check_airflow_task_args(func, args, kwargs):
        r"""
        This is a method decorator to know which arguments correspond to the task, and which are of the function.

        Every method compatible with airflow task must be decorated with this decorator.

        ```python
        from airflow_df.helpers import Helpers

        @Helpers.as_airflow_tasks()
        class IO:

            @Helpers.check_airflow_task_args
            @staticmethod
            def read_csv(filepath:str, **kwargs)->pd.DataFrame:
                
                pass
        ```
        """
        _kwargs = kwargs.copy()
        default_args = dict()
        for key in _kwargs.keys():

            if key in Helpers.get_default_args_in_tasks():

                default_args[key] = kwargs.pop(key)

        result = func(*args, **kwargs)

        return result

    @decorator
    @staticmethod
    def hide_airflow_compatibility(func, args, kwargs):
        r"""
        This is a method decorator to hide airflow compatibility.

        ```python
        from utils.helpers import Helpers

        @Helpers.hide_airflow_compatibility
        @staticmethod
        def read_csv(filepath:str, **kwargs)->pd.DataFrame:
            
            pass
        ```
        """
        _kwargs = kwargs.copy()
        default_args = dict()
        for key in _kwargs.keys():

            if key in Helpers.get_default_args_in_tasks():

                default_args[key] = kwargs.pop(key)

        task = func(*args, **kwargs)
        task = task.operator.python_callable
        return task(*args[1:], **kwargs)

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
        Helpers._check_file_size_recursively(filepath)
        result = list()
        if not ext.startswith("."):
            ext = f".{ext}"
        
        filepath = filepath.split(os.sep)
        filepath = os.sep.join(filepath)
        for root, dirnames, filenames in os.walk(filepath):
            for filename in filenames:
                if filename.endswith(ext):

                    result.append(os.path.join(root,filename))
        
        return result

    @staticmethod
    def _check_file_size_recursively(file_path) -> None:
        """
        Recursively checks the size of files in the specified directory and its subdirectories.
        If any file has a size less than 5 bytes, it raises an exception along with the list of
        files without size.

        Args:
            file_path (str): The path to the directory to be checked.

        Raises:
            Exception: If there are files without size, it raises an exception with the list of
                    files without size.

        Returns:
            None
        """
        files_without_size = []

        try:
            for root, dirs, files in os.walk(file_path):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    if os.path.isfile(filepath):
                        file_size = os.path.getsize(filepath)
                        if file_size < 5:
                            files_without_size.append(filename)

        except Exception as error:
            raise error

        if(len(files_without_size)>0):

            for f in files_without_size:
                print(f"This file {f} is empty, please check it and remove it.")

            raise Exception("There are files without size, please check them and remove them.")

    @staticmethod
    def get_default_args_in_tasks():
        r"""
        Helps to you which arguments are default for airflow tasks.
        """
        return [
            'conf',
            'dag', 
            'dag_run',
            'data_interval_end',
            'data_interval_start',
            'ds',
            'ds_nodash',
            'execution_date',
            'expanded_ti_count',
            'inlets',
            'logical_date',
            'macros',
            'next_ds',
            'next_ds_nodash',
            'next_execution_date',
            'outlets',
            'params',
            'prev_data_interval_start_success',
            'prev_data_interval_end_success',
            'prev_ds',
            'prev_ds_nodash',
            'prev_execution_date',
            'prev_execution_date_success',
            'prev_start_date_success',
            'run_id',
            'task',
            'task_instance',
            'task_instance_key_str',
            'test_mode',
            'ti',
            'tomorrow_ds',
            'tomorrow_ds_nodash',
            'triggering_dataset_events',
            'ts', 
            'ts_nodash', 
            'ts_nodash_with_tz',
            'var',
            'conn',
            'yesterday_ds',
            'yesterday_ds_nodash',
            'templates_dict'
        ]
    