#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
from builtins import range
from pprint import pprint
from airflow.utils.dates import days_ago
from datetime import timedelta
from textwrap import dedent
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator


# In[ ]:


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'ETL+LDA',
    default_args=default_args,
    description='DAG for ETL and Topic analysis',
    schedule_interval='00 9 * * *',
    start_date=days_ago(2),
    tags=['trial1'],
) as dag:
    t1 = BashOperator(
        task_id='ETL',
        depends_on_past=False,
        bash_command='/Users/molly1998/Desktop/python/airflow/dags/scripts/ETL.py',
    )
    
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command='/Users/molly1998/Desktop/python/airflow/dags/scripts/preprocess-for-lda-pipeline.py',
    )
    
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command='/Users/molly1998/Desktop/python/airflow/dags/scripts/lda-model.py',
        ##edit hyper-parameter here
        params={'num_topics': 10,'iterate_times':10},
    )
    
    t1 >> t2 >> t3

