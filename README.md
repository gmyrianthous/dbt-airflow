# dbt-airflow
A Python package that creates fine-grained dbt tasks on Apache Airflow. 


---

# Installation

The package is available on PyPI and can be installed through `pip`:
```bash
pip install dbt-airflow
```


---

# Contributing
This project uses `poetry` to manage dependencies and package releases.
Therefore, you should first install `poetry` via `pip`: 
```bash
$ pip install --upgrade pip
$ pip install poetry==1.2.0
```

Then install the dependencies:
```bash
$ poetry install
```

To run all the tests
```bash
poetry run pytest tests -vv
```

Running a specific test(s)
```bash
# Will match all tests starting with test_name
poetry run pytest tests -k "test_name" -vv
```