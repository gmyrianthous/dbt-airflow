# dbt-airflow
A Python package that creates fine-grained dbt tasks on Apache Airflow.

## How does it work
The library essentially builds on top of the metadata generated by `dbt-core` and are stored in 
the `target/manifest.json` file in your dbt project directory.  

## Domain Requirements

- Every dbt project, when compiled, will generate a metadata file under `<dbt-project-dir>/target/manifest.json`
- The manifest file contains information about the interdependencies of the project's data models
- `dbt-airflow` aims to extract these dependencies such that every dbt entity (snapshot, model, test and seed) has 
  its own task in a Airflow DAG while entity dependencies are persisted
- Snapshots are never an upstream dependency of any task
- The creation of snpashots on seeds does not make sense, and thus not handled 
  not even sure if this is even possible on dbt side)
- Models may have tests
- Snapshots may have tests
- Seeds may have tests

---

# Installation

The package is available on PyPI and can be installed through `pip`:
```bash
pip install dbt-airflow
```

`dbt` needs to connect to your target environment (database, warehouse etc.) and in order to do so, it makes use of 
different adapters, each dedicated to a different technology (such as Postgres or BigQuery). Therefore, before running
`dbt-airflow` you also need to ensure that the required adapter(s) are installed in your environment. 

For the full list of available adapters please refer to the official 
[dbt documentation](https://docs.getdbt.com/docs/available-adapters). 

---

# Usage
`dbt-airflow` can be used either as a normal Python package, or through the 
command line interface. 

Given that there are possibly many different ways for deploying Airflow and automating different aspects
of data workflows that involve Airflow, dbt and potentially other tools as well, we wanted to offer more
flexibility by providing different approaches for using `dbt-airflow`.

### Building an Airflow DAG using `dbt-airflow`

```python3
from datetime import datetime
from pathlib import Path

from airflow import DAG

from dbt_airflow.dag_builder import build_dag


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    build_dag(
        dbt_manifest_path=Path('path/to/dbt/project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('path/to/dbt/project'),
        dbt_profile_path=Path('path/to/dbt/profiles),
    )
---

# Contributing
If you would like to contribute to `dbt-airflow` project, you will essentially need to follow the steps outlined below:
1. Create a fork of the repository
2. Set up the development environment on your local machine (see the detailed guide below)
3. Write and test your contribution
4. Create a Pull Request

##  Setting up your local development environment
In order to be able to contribute to the project, you need to set up a proper development environment where
you will be able to install all the dependencies required in order to run and test new functionality or bug fixes. 

To do so, you first need to create and then activate a virtual environment:
```bash
$ python3 -m venv ~/dbt-airflow-venv
$ source ~/dbt-airflow-venv/bin/activate
```

Now once you have activated your environment, you'll then need to install `dbt-airflow` in **editable mode**. 
When this mode is activated, the package will be linked to the specified directory (which should be the directory 
containing the package under development) such that any changes made in the package will be reflected directly 
in your local environment. Apart from project dependencies, you also need to install the extra `test` dependencies 
(these are dependencies  which are used as part of testing, such as `pytest`).

You can do so using the `-e` (or `--editable`) flag when installing the package through `pip`:
```bash
python3 -m pip install -e .[test]
```

The above command will install all the dependencies specified in `install_requires` of setup files as well as the 
`extras_require.test` dependencies.
