#!/usr/bin/env python
# coding: utf-8

import time
from builtins import range
from pprint import pprint
from airflow.utils.dates import days_ago
from datetime import timedelta
from textwrap import dedent
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator


default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id='ETL_LDA',
    default_args=default_args,
    description='DAG for ETL and Topic analysis',
    schedule_interval='00 9 * * *',
    tags=['trial1'],
) as dag:
    t1 = BashOperator(
        task_id='ETL',
        bash_command='/Users/molly1998/Desktop/python/airflow/pyForDag/ETL1.py',
    )
    
    t2=PythonOperator(
        task_id='ldaCV',
        python_callable=ldaCV,
        provide_context=True,
    )
    
    
    t3 = BashOperator(
        task_id='lda_model',
        bash_command='python /Users/molly1998/Desktop/python/airflow/pyForDag/lda.py {{ task_instance.xcom_pull(task_ids='cross_validation') }} ',
        provide_context=True,
    )
    
    t4 = BashOperator(
        task_id='produce_dashboard',
        bash_command='/Users/molly1998/Desktop/python/airflow/pyForDag/dashboard.py',
    )
    
    t1 >> t2 >> t3 >> t4

