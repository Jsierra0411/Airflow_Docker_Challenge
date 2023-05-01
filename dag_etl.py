
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Jorge Sierra',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def extraction(ti):
  df_traffic = pd.read_csv('C:\Users\georg\Downloads\Traffic_Flow_Map_Volumes.csv')
  ti.xcom_push(key = 'df_traffic', value = df_traffic )
  
def transformation(ti):
  dti = ti.xcom_pull(task_ids='extraction', key='df_traffic')
  ds_0_0_0_ = dti.dropna()
  ds_0_0_ = ds_0_0_0_.groupby('AAWDT').count()
  ds_0_ = ds_0_0_[ds_0_0_.columns[-1:]]
  ds = ds_0_.rename(columns = {'INPUT_STUDY_ID': 'TOTAL_ACCIDENTS_PER_W'})
  ti.xcom_push(key = 'ds', value = ds)

def new_csv(ti):
   dti_2 = ti.xcom_pull(task_ids='transformation', key='ds')
   return dti_2.to_csv('Weather_accident.csv')

with DAG(
    default_args=default_args,
    dag_id='etl weather V1',
    description='etl weather accident',
    start_date=datetime(2021, 10, 6),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id='extraction',
        python_callable=extraction,
    )

    task2 = PythonOperator(
        task_id='transformation',
        python_callable=transformation
    )

    task3 = PythonOperator(
        task_id='new_csv',
        python_callable=new_csv
    )

    task1 >> task2
    task2 >> task3



