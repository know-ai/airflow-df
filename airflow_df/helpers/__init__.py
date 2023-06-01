import inspect
from airflow.decorators import task


def as_airflow_tasks():

    def decorate(cls):

        attrs = inspect.getmembers(cls, predicate=lambda x: inspect.isroutine(x))

        for attr, _ in attrs:
            
            if not attr.startswith('_'):

                setattr(cls, attr, task(getattr(cls, attr)))
        
        return cls

    return decorate