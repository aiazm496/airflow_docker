from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

import pendulum


with DAG('dag_1', description='first dag',
         schedule_interval=timedelta(minutes=2),
         start_date= pendulum.datetime(2022,6,11,15,2,tz='Asia/Calcutta'),  #the dag task will start executing from start_date + schedule_interval
        #  end_date=pendulum.datetime(2022,6,11,15,2,tz='Asia/Calcutta'),
         catchup=False,
         default_args={
             'depends_on_past': False,
             'email': 'aiazm496@gmail.com',
             'email_on_failure': True,

             'email_on_retry': True,
             'retries': 1,
             'retry_delay': timedelta(seconds=3)
         }

         ) as dag:

    t1 = BashOperator(
        task_id='task_1',
        bash_command='echo date'
    )

    t2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 5',
        execution_timeout = timedelta(seconds=3), #it will fail as timeout is 3 sec and command will run for 5 sec
        retries=3  #will retry 3 times
    )

    t3 = BashOperator(
        task_id='task_3',
        bash_command='echo task3'

    )

    t1 >> [t2, t3]
