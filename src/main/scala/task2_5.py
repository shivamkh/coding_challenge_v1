import datetime as dt

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

dag = DAG(
    dag_id='branch_dummy',
    schedule_interval='@once',
    start_date=dt.datetime(2019, 2, 28)
)



Test_1 = DummyOperator(task_id='Test_1', dag=dag)


Test_2 = DummyOperator(task_id='Test_2', dag=dag)
Test_3 = DummyOperator(task_id='Test_3', dag=dag)

Test_5 = DummyOperator(task_id='Test_5', dag=dag)
Test_6 = DummyOperator(task_id='Test_6', dag=dag)
Test_7 = DummyOperator(task_id='Test_7', dag=dag)

join = DummyOperator(task_id='join', dag=dag)

Test_1 >> Test_2 >> join
Test_1 >> Test_3 >> join

join >> Test_5
join >> Test_6
join >> Test_7


                                                              

