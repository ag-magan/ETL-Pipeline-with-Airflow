from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from weather import weather_forecast

# Default arguments for DAG:
# start_date : date from when we need to start processing.
# end_data   : date until when we need to process the data.
# depends_on_past : this DAG is independent of previous runs.
# email_on_retry : We don't want emails on retry.
# retries : Number of times the task is retries upon failure.
# retry_delay : How much time should the scheduler wait before re-attempt.
# provide_context : When you provide_context=True to an operator, we pass
#   along the Airflow context variables to be used inside the operator.
default_args = {
    'owner': 'agmagan',
    'start_date': datetime(2021, 6, 27),
    'end_date': datetime(2021, 12, 31),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

# Dag will start automatically when turned on
# This is because the start date is in the past
dag = DAG(
        'weather_report',
        default_args=default_args,
        description='Send weather forecasts',
        schedule_interval='@daily',
        max_active_runs=1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

extract_and_load_weather_data = PythonOperator(
    task_id="extract_and_load_weather_data",
    python_callable=weather_forecast,
    dag=dag
)

precheck = FileSensor(
   task_id='check_for_weatherfile',
   filepath='weather_forecast.html',
   poke_interval=10,
   timeout=60 * 30,
   dag=dag
)

email_task = EmailOperator(    
    task_id='email_weather_data',    
    to='agn.magan@gmail.com',    
    subject='Forecast for the day',    
    html_content='',
    files='weather_forecast.html',    
    dag=dag
)

end_operator = DummyOperator(task_id='End_Execution', dag=dag)

start_operator >> extract_and_load_weather_data
extract_and_load_weather_data >> precheck
precheck >> email_task
email_task >> end_operator
