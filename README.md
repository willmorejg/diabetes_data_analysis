# diabetes_data_analysis

## Table of Contents

- [diabetes\_data\_analysis](#diabetes_data_analysis)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
  - [Prerequisites](#prerequisites)
  - [Installing](#installing)
  - [Usage Preparation](#usage-preparation)
  - [Usage](#usage)

## About

This project contains python code to analyze data related to Type 1 diabetes.

## Prerequisites

python needs to be installed on the system in order to run any of the scripts / code.

## Installing

Setup the python virtual environment with the operating system as linux, and activate it

```
python -m venv .venv
source .venv/bin/activate
```

install setuptools dependency

```
pip install --upgrade setuptools
```

Install dependencies

```
pip install -e .
```

Install development dependencies (optional)

```
pip install -e .[dev]
```

## Usage Preparation

Create a .env file with the following values (if persisting data to a PostgreSQL database) (optional)

```
server=localhost #database server host
database=jim #database name
username=foo #database user
password=password #database user's password
schema=diabetes #database schema name
```

In the sql directory there is a DDL (data definition language) SQL script that will create the database table that is used for persisting data into a PostgreSQL database.

## Usage

The execution of the script to read the data export and insert records into the database

```
python src/persist_data.py
```