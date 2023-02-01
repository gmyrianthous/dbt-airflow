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

RUN pip install poetry

RUN poetry install
