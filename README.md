# Pentaho Airflow plugin

[![Build Status](https://travis-ci.org/damavis/airflow-pentaho-plugin.svg?branch=master)](https://travis-ci.org/damavis/airflow-pentaho-plugin)

This plugin runs Pan (transformations) and Kitchen (Jobs) in PDI
repository or local XML files. It allows to orchestrate a massive
number of trans/jobs taking care of the dependencies between them,
even between different instances.

## Requirements

1. One or many working PDI CE installations.
2. A Apache Airflow system deployed.

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

Enter the **Conn Id**, this plugin uses `pdi_default` by default, the username
and the password for your Pentaho Repository.

At the bottom of the form, fill the **Extra** field with `pentaho_home`, the
path where your pdi-ce is placed, and `rep`, the repository name for this
connection, using a json formatted string like it follows.

```json
{"pentaho_home": "/opt/pentaho", "rep": "Default"}
```

## Usage

### KitchenOperator

Kitchen operator is responsible for running Jobs. Lets suppose that we have
a defined *Job* saved on `/home/bi/average_spent` in our repository with
the argument `date` as input parameter. Lets define the task using the
`KitchenOperator`.

```python
from airflow.operators.pentaho import KitchenOperator

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

### PanOperator

Pan operator is responsible for running transformations. Lets suppose that
we have one saved on `/home/bi/clean_somedata`. Lets define the task using the
`PanOperator`. In this case, the transformation receives a parameter that
determines the file to be cleaned.

```python
from airflow.operators.pentaho import PanOperator

# ... #

# Define the task using the KitchenOperator
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
