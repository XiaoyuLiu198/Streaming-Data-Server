#!/usr/bin/env python
# coding: utf-8

from functools import partial
import functools
from inspect import signature
import sys
import time
from builtins import range
from pprint import pprint
from datetime import timedelta
from textwrap import dedent
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator



def Dag(dag_id=None, schedule_interval='@daily', start_date=pendulum.datetime(2022, 2, 1, tz="UTC"), catchup=False, tags=['test'],) -> Callable[[Callable], Callable[..., DAG]]:
    """
    Personalized dag decorator for wrapping function into DAG object
    """
    
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            f_sig = signature(f).bind(*args, **kwargs)
            f_sig.apply_defaults()
            with DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date, catchup=catchup, tags=tags) as dag_obj:
                if f.__doc__:
                    dag_obj.doc_md = f.__doc__
                f_kwargs = {}
                for name, value in f_sig.arguments.items():
                    f_kwargs[name] = dag_obj.param(name, value)

                back = sys._getframe().f_back
                dag_obj.fileloc = back.f_code.co_filename if back else ""

                # Invoke function to create operators in the DAG scope.
                f(**f_kwargs)

            return dag_obj
        return wrapper
    return decorator
            
@Dag(dag_id='ETL_LDA', schedule_interval='@daily', tags=['trial'])
def etl():
    """ETL operators, after applying the Dag decorator, this would be wrapped to a DAG object"""
    t1 = BashOperator(
        task_id='ETL',
        bash_command='python pyForDag/ETL.py',
    )

    t2=PythonOperator(
        task_id='ldaCV',
        python_callable=ldaCV,
        provide_context=True,
    )

    t3 = BashOperator(
        task_id='lda_model',
        bash_command='python pyForDag/lda-pyspark.py {{ task_instance.xcom_pull(task_ids="cross_validation") }} ',
        provide_context=True,
    )
    
    t4 = BashOperator(
        task_id='produce_dashboard',
        bash_command='python pyForDag/dashboard.py',
    )
    
    t1 >> t2 >> t3 >> t4

etl()


