#airflow dag which fails if the post with id not found

from datetime import timedelta
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests
import random



def post_get():
    number = random.randrange(1,200)  #our posts id are only from 1 to 100, so it will fail post ids > 100
    print('number',number)
    api = f'https://jsonplaceholder.typicode.com/posts/{number}'
    response = requests.get(api)
    print(response.text)
    response_dict = response.json() 
    print('len :',response_dict)
    return len(response_dict)     #if an empty dict is returned then len returned will be 0


def post_validate(ti):
    
    len_of_task_get_post  = ti.xcom_pull(task_ids='get_post')
    if len_of_task_get_post ==0:
        return 'fail'
    return 'pass'

    

with DAG(dag_id = 'dag_3', description= 'This dag task fails if post not found for an id',
        start_date=pendulum.datetime(2022,6,17,23,42,tz='Asia/Calcutta'),
        schedule_interval=timedelta(seconds=60),
        catchup=False,

        default_args={
            'retries':1,
            'retry_delay': timedelta(seconds=3)
        }
) as dag:
    

    get_post = PythonOperator(
        task_id = 'get_post',
        python_callable=post_get
    )

    validate_post = BranchPythonOperator(
        task_id = "validate_post",
        python_callable=post_validate
    )

    fail = BashOperator(
        task_id = 'fail',
        bash_command='echo fail'
    )

    passed = BashOperator(
        task_id = 'pass',
        bash_command='echo pass'
    )

    get_post >> validate_post >> [fail,passed]