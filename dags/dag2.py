from airflow import DAG 

from datetime import timedelta

import pendulum

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
import random


def task_1():
    list_1 = []
    for i in range(5):
        list_1.append(random.randrange(1,100))  #random no between 1 and 99 (100 is excluded)
    return list_1


def check(ti):
    list_2 = ti.xcom_pull(task_ids='task_1')
    print('length of list',len(list_2)) #check in logs of a task
    if(len(list_2)==5):
        return 'success' #define this task
    else:
        return 'fail'    #define this task


with DAG(dag_id='dag_2',description='second dag',
        start_date=pendulum.datetime(2022,6,11,17,9,tz='Asia/Calcutta'),
        schedule_interval=timedelta(minutes=2),
        catchup=False,
        default_args={ #can override in task
            'depends_on_past': False, 
            'retries':1,
            'retry_delay': timedelta(seconds=3),

        } 
        ) as dag:
        

        t1 = PythonOperator(
            task_id='task_1',
            python_callable=task_1,
            
        )


        t2 = BranchPythonOperator(
            task_id= "task_2",
            python_callable=check
        )


        t3 = BashOperator(
            task_id ='success',
            bash_command='echo success'
        )

        t4 = BashOperator(
            task_id = 'fail',
            bash_command='echo fail'
        )

        t1 >> t2 >> [t3,t4]