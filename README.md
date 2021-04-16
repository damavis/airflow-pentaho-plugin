# Pentaho Airflow plugin

[![Build Status](https://travis-ci.org/damavis/airflow-pentaho-plugin.svg?branch=master)](https://travis-ci.org/damavis/airflow-pentaho-plugin)
[![codecov](https://codecov.io/gh/damavis/airflow-pentaho-plugin/branch/master/graph/badge.svg)](https://codecov.io/gh/damavis/airflow-pentaho-plugin)
[![PyPI](https://img.shields.io/pypi/v/airflow-pentaho-plugin)](https://pypi.org/project/airflow-pentaho-plugin/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airflow-pentaho-plugin)](https://pypi.org/project/airflow-pentaho-plugin/)

This plugins runs Jobs and Transformations through Carte servers.
It allows to orchestrate a massive number of trans/jobs taking care
of the dependencies between them, even between different instances.
This is done by using `CarteJobOperator` and `CarteTransOperator`

It also runs Pan (transformations) and Kitchen (Jobs) in local mode,
both from repository and local XML files. For this approach, use
`KitchenOperator` and `PanOperator`

## Requirements

1. A Apache Airflow system deployed.
2. One or many working PDI CE installations.
3. A Carte server for Carte Operators.

## Setup

The same setup process must be performed on webserver, scheduler
and workers (that runs this tasks) to get it working. If you want to
deploy specific workers to run this kind of tasks, see
[Queues](https://airflow.apache.org/docs/stable/concepts.html#queues),
in **Airflow** *Concepts* section.

### Pip package

First of all, the package should be installed via `pip install` command.

```bash
pip install airflow-pentaho-plugin
```

### Airflow connection

Then, a new connection needs to be added to Airflow Connections, to do this,
go to Airflow web UI, and click on `Admin -> Connections` on the top menu.
Now, click on `Create` tab.

Use HTTP connection type. Enter the **Conn Id**, this plugin uses `pdi_default`
by default, the username and the password for your Pentaho Repository.

At the bottom of the form, fill the **Extra** field with `pentaho_home`, the
path where your pdi-ce is placed, and `rep`, the repository name for this
connection, using a json formatted string like it follows.

```json
{
    "pentaho_home": "/opt/pentaho",
    "rep": "Default"
}
```

### Carte

In order to use `CarteJobOperator`, the connection should be set different. Fill
`host` (including `http://` or `https://`) and `port` for Carte hostname and port,
`username` and `password` for PDI repository, and `extra` as it follows.

```json
{
    "rep": "Default",
    "carte_username": "cluster",
    "carte_password": "cluster"
}
```

## Usage

### CarteJobOperator

CarteJobOperator is responsible for running jobs in remote slave servers. Here
it is an example of `CarteJobOperator` usage.

```python
# For versions before 2.0
# from airflow.operators.airflow_pentaho import CarteJobOperator

from airflow_pentaho.operators.carte import CarteJobOperator

# ... #

# Define the task using the CarteJobOperator
avg_spent = CarteJobOperator(
    conn_id='pdi_default',
    task_id="average_spent",
    job="/home/bi/average_spent",
    params={"date": "{{ ds }}"},  # Date in yyyy-mm-dd format
    dag=dag)

# ... #

some_task >> avg_spent >> another_task
```

### KitchenOperator

Kitchen operator is responsible for running Jobs. Lets suppose that we have
a defined *Job* saved on `/home/bi/average_spent` in our repository with
the argument `date` as input parameter. Lets define the task using the
`KitchenOperator`.

```python
# For versions before 2.0
# from airflow.operators.airflow_pentaho import KitchenOperator

from airflow_pentaho.operators.kettle import KitchenOperator

# ... #

# Define the task using the KitchenOperator
avg_spent = KitchenOperator(
    conn_id='pdi_default',
    queue="pdi",
    task_id="average_spent",
    directory="/home/bi",
    job="average_spent",
    params={"date": "{{ ds }}"},  # Date in yyyy-mm-dd format
    dag=dag)

# ... #

some_task >> avg_spent >> another_task
```

### CarteTransOperator

CarteTransOperator is responsible for running transformations in remote slave
servers. Here it is an example of `CarteTransOperator` usage.

```python
# For versions before 2.0
# from airflow.operators.airflow_pentaho import CarteTransOperator

from airflow_pentaho.operators.carte import CarteTransOperator

# ... #

# Define the task using the CarteJobOperator
enriche_customers = CarteTransOperator(
    conn_id='pdi_default',
    task_id="enrich_customer_data",
    job="/home/bi/enrich_customer_data",
    params={"date": "{{ ds }}"},  # Date in yyyy-mm-dd format
    dag=dag)

# ... #

some_task >> enrich_customers >> another_task
```

### PanOperator

Pan operator is responsible for running transformations. Lets suppose that
we have one saved on `/home/bi/clean_somedata`. Lets define the task using the
`PanOperator`. In this case, the transformation receives a parameter that
determines the file to be cleaned.

```python
# For versions before 2.0
# from airflow.operators.airflow_pentaho import PanOperator

from airflow_pentaho.operators.kettle import PanOperator

# ... #

# Define the task using the PanOperator
clean_input = PanOperator(
    conn_id='pdi_default',
    queue="pdi",
    task_id="cleanup",
    directory="/home/bi",
    trans="clean_somedata",
    params={"file": "/tmp/input_data/{{ ds }}/sells.csv"},
    dag=dag)

# ... #

some_task >> clean_input >> another_task
```

For more information, please see `sample_dags/pdi_flow.py`
