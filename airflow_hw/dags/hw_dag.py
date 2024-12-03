import datetime as dt
import os
import sys
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG

from modules.predict import predict
from modules.pipeline import pipeline





# Указываем путь к проекту
project_path = '/opt/airflow/airflow_hw'
print(project_path)
os.environ['PROJECT_PATH'] = project_path
sys.path.insert(0, project_path)



# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

# Определяем DAG
with DAG(
    dag_id='car_price_prediction',
    default_args=default_args,
    schedule_interval="00 15 * * *",  # Ежедневно в 15:00
) as dag:
    # Задача 1: запуск pipeline
    run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=pipeline,
    )

    # Задача 2: запуск predict
    run_predict = PythonOperator(
        task_id='run_predict',
        python_callable=predict,
    )

    # Определяем порядок выполнения задач
    run_pipeline >> run_predict



