##
#  This dockerfile is used for local development and testing
##
FROM apache/airflow:2.5.0

USER root

RUN sudo apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3-distutils \
    libpython3.9-dev

USER airflow

COPY . .

# We've had issues disabling poetry venv
# See: https://github.com/python-poetry/poetry/issues/1214
RUN python -m pip install .
