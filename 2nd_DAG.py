from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#First we write here our python logic

#Defining a function 

def python_first_function():
    print("The current date and time is:", datetime.now())


# create a Dag which calls the python logic that you had created above

default_dag_args = {
    "start_date": datetime(2022,1,1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "project_id":1
}

# https://crontab.guru/ can be useful
with DAG("first_python_DAG",schedule_interval= "@daily",catchup= False,default_args = default_dag_args) as dag_python:

    #here we define our tasks
    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)