# Contributing to dbt-airflow
We encourage everyone to be part of this journey and contribute to the project. You can do so by implementing new
features, fixing potential bugs or even improving our documentation and CI/CD pipelines. If you are planning to 
contribute with the implementation of a new feature, you should first open an Issue on GitHub where you can share your
ideas and designs so that project's contributors and maintainers can provide their insights. This will ensure that you
won't be spending your time on implementing a piece of work that might not support the longer vision of the project and
unavoidably might not be merged. 


## Setting up a local environment
The next few sections will help you set up a local development environment where you can quickly test your changes, 
before opening a Pull Request. 

### Creating a fork 

The first thing you need to do, is to create a fork of the repository. You can do so by clicking on the `Fork`
button that is found on the top right corner of the project's page on GitHub. Once you create a fork, you'll then have
to clone your fork onto your local machine. 

### Setup your local environment
In your forked project's directory, create and activate a fresh virtual environment:
```bash
# Create a virtual environment called `dbt-airflow-venv`
$ python3 -m venv ~/dbt-airflow-venv

# Activate the newly created virtual environment
$ source ~/dbt-airflow-venv/bin/activate
```

Every single merge into `main` branch will trigger a new patch, minor or major version upgrade based on the commit 
messages pushed  from the Pull Request.
The automated release mechanism is based on 
[conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#summary).
Every single commit must follow the structural elements described in Conventional Commits' specification. 
The repository also contains pre-commit hooks that will ensure compliance to the specification. 
Make sure to install pre-commit hooks to avoid any inconsistencies, by following the steps outlined below. 
```bash
# Install `pre-commit` package from PyPI
$ python3 -m pip install pre-commit 

# Install hooks from `.pre-commit-config.yaml`
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

### Testing your changes locally
In order to see how your changes will be reflected on Airflow, you can spin up the docker containers from the images
specified in `docker-compose.yml` and `Dockerfile` files. 

```bash
# Build the images (you can omit `--no-cache` in case you don't want to re-build every layer)
$ docker compose build --no-cache

# Run the containers
$ docker compose up
```
Basically, the commands above will spin up the following containers:
- An Airflow instance whose webserver can be accessd on `localhost:808` (use `airflow` and `airflow` in user/pass credentials)
- A postgres instance containing the popular Sakila data, where dbt models can materialize
- A container that gives you access to `dbt` CLI where you can run further `dbt` commands

The Postgres instance can be accessed in the following way (note that default port was changed to `5433` given that we 
have an additional postgres instance for Airflow itself):
```bash
# Get the id of the running postgres-sakila container
$ docker ps

# Enter the running container
$ docker exec -it <container-id> /bin/bash

# Enter psql
$ psql -U postgres -p 5433 
```

You will now be able to run Airflow DAGs authored with the use of `dbt-airflow` where you can also evaluate results
either on the Airflow UI (webserver) or on the local database itself. 




Additionally, you need to make sure that all tests pass successfully. This project uses `poetry` tool for 
dependency management. You'll have to install `poetry` and install the dependencies specified in `pyproject.toml`. 
```bash
# Install poetry
$ python3 -m pip install poetry==1.3

# Install dependencies
$ poetry install
```

If you'd like to run the tests, make sure to do so within the poetry environment, as  shown below. 
```bash
# Run all tests
poetry run pytest tests 

# Run test(s) with specific prefix or specific name
poetry run pytest tests -k "test_some_prefix_or_full_test_name"
```


### Opening a Pull Request


