from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_dag_args = {
'owner': 'me',
'start_date': datetime(2022, 1, 1),
'email_on_failure' : False,
'email_on_retry' : False,
'retries': 1,
'retry_delay': timedelta(minutes=5)
}

#Dag = DAG(
    #'my_dag_id',
    #default_args=default_dag_args,
    #schedule_interval=timedelta(hours=1),
#)

# Let's define our dag
with DAG('First_DAG', schedule_interval=None, default_args=default_dag_args) as dag:
    # tasks for the DAG go here
    # here at this level we define our tasks for dag
    task_0= BashOperator(task_id= 'bash_task', bash_command = "echo 'command executed from Bash Operator' ")
    task_1= BashOperator(task_id= 'bash_task_move_data', bash_command = "copy C:/Users/kulpsales2/Documents/DATA_CENTER/DATA_LAKE/dataset_raw.txt  C:/Users/kulpsales2/Documents/DATA_CENTER/CLEAN_DATA")
    task_3 = BashOperator(task_id = 'bash_task_remove_raw_data', bash_command = "rm C:/Users/kulpsales2/Documents/DATA_CENTER/dataset_raw.txt")
    task_0 >> task_1 >> task_3