# airflow-df

This package helps to integrate Pandas DataFrame operations for ETL process with Apache Airflow Pipelines

[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow’s extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single process on your laptop to a distributed setup to support even the biggest workflows.

# Workflow As Code

The main characteristic of Airflow workflows is that all workflows are defined in Python code. “Workflows as code” serves several purposes:

- **Dynamic:** Airflow pipelines are configured as Python code, allowing for dynamic pipeline generation.

- **Extensible:** The Airflow framework contains operators to connect with numerous technologies. All Airflow components are extensible to easily adjust to your environment.

- **Flexible:** Workflow parameterization is built-in leveraging the Jinja templating engine.

Take a look at the following snippet of code:

```python
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow()

```

Here you see:

- A DAG named “demo”, starting on Jan 1st 2022 and running once a day. A DAG is Airflow’s representation of a workflow.

- Two tasks, a BashOperator running a Bash script and a Python function defined using the @task decorator

- ">>" between the tasks defines a dependency and controls in which order the tasks will be executed

Airflow evaluates this script and executes the tasks at the set interval and in the defined order. The status of the “demo” DAG is visible in the web interface

![demo graph view](docs/img/demo_graph_view.png)

# Goal

The initial goal of this project is to abstract the ETL (Extraction - Transform - Load) process using Pandas DataFrame, in such a way that an end user who is not an expert in airflow or pandas, can create DAGs through YaML configuration files.

Initially, the goal is to use the airflow 'task' decorator to connect the different stages of the pipeline.

Therefore, *airflow-df* must package a series of modules to achieve its goal.

These modules are:

- **io:** (Input/Ouput) This module is responsible of the data extraction functions from different sources and convert them to DataFrames for their subsequent transformation, as well as the data load operations after transforming them to different sources, csv, databases, pickles, among others.

- **transform:** This module allows all the necessary transformations that must be done to the loaded DataFrame.


- **dag:** The main goal of this module should be to create the DAG file needed by airflow from a configuration file.


- **helpers:** Any other functionality that is useful for the library must be developed in this module.


# Considerations

There are several ideal considerations in the library that developers should take into account when successfully collaborating on the project.

If the developer wants to package a series of methods in some class, and for it to be compatible with airflow, the following must be followed:

- Every class must be decorated with the "as_airflow_tasks" decorator. According to the following snippet:

```python
from helpers import Helpers

@Helpers.as_airflow_tasks()
class IO:
```

- Once the class is decorated, any method that does not start with *_* will be airflow compatible.

- In addition to the previous point, any method that requires airflow compatibility must be static and decorated with the *check_airflow_task_args* decorator.

```python
@Helpers.check_airflow_task_args
@staticmethod
def read_csv(filepath:str, **kwargs)->pd.DataFrame:
```


# Testing

Unittest are executed with:

```python
python3 tests.py
```

# Documentation

The specification used is [MkDocs](https://www.mkdocs.org/getting-started/).

Install the dependencies for documentation:

```python
pip install -r docs_requirements.txt
```

MkDocs comes with a built-in dev-server that lets you preview your documentation as you work on it. Make sure you're in the same directory as the mkdocs.yml configuration file, and then start the server by running the mkdocs serve command:

```python
mkdocs serve
```

# Collaboration

Fork this repo and then clone it from your github to local.