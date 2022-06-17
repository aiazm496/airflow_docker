#dag file
# It’s a DAG definition file
# One thing to wrap your head around (it may not be very intuitive for everyone at first) is that this Airflow Python script is 
# really just a configuration file specifying the DAG’s structure as code. The actual tasks defined here will run in a different 
# context from the context of this script. Different tasks run on different workers at different points in time, which means that
# this script cannot be used to cross communicate between tasks. Note that for this purpose we have a more advanced feature called XComs.

# People sometimes think of the DAG definition file as a place where they can do some actual data processing - that is not 
# the case at all! The script’s purpose is to define a DAG object. It needs to evaluate quickly (seconds, not minutes) since the
# scheduler will execute it periodically to reflect the changes if any

from random import randint
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def training_model():
    return randint(1,10)

def choose_best_model(ti):  #using task instance of task choose_best_model, to do xcom pull to communicate with other tasks
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])


    best_accuracy = max(accuracies)
    if best_accuracy > 8: # we will use xcom to read data from task execution which is stored in metadata
        return 'accurate'  #the function must return the next task id to execute
    return 'inaccurate'

with DAG("my_dag",start_date=datetime(2022,6,7),schedule_interval='@daily',catchup=False) as dag: #airflow creates dag obj when it runs
    #in our dag object , we need to create tasks model a,b and c. each task is defined by operator
    #training_model_A task

    #An object instantiated from an operator is called a task
    training_model_A = PythonOperator(   #task  each task runs on diff machine/workers and an task instance object is created
        task_id = "training_model_A",
        python_callable=training_model
    )

    training_model_B = PythonOperator(  #task 
        task_id = "training_model_B",
        python_callable=training_model
    )

    training_model_C = PythonOperator( #task
        task_id = "training_model_C",
        python_callable=training_model
    )

    choose_best_model = BranchPythonOperator( #to find task with best accuracy and execute next task
        task_id = "choose_best_model",
        python_callable=choose_best_model #the python function must return the tasks (task id to execute # based on a condition)
    )

    accurate = BashOperator( #to execute bash command
        task_id="accurate",
        bash_command='echo accurate'
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command='echo inaccurate'
    )

    #to set order and dependencies of tasks
    [training_model_A,training_model_B,training_model_C] >> choose_best_model >> [accurate,inaccurate]


