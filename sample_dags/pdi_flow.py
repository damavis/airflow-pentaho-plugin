# -*- coding: utf-8 -*-
# Copyright 2020 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.pentaho import KitchenOperator

DAG_NAME = "pdi_flow"
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

    job1 = KitchenOperator(
        dag=dag,
        task_id="job1",
        xcom_push=True,
        directory="/home",
        job="test_job",
        params={"date": "{{ ds }}"})

    job2 = KitchenOperator(
        dag=dag,
        task_id="job2",
        xcom_push=True,
        directory="/home",
        job="test_job",
        params={"date": "{{ ds }}"})

    job1 >> job2
