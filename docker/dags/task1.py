from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


with DAG(
    'push_multiple_var',
    description='Push multiple variable to xcom',
    start_date= datetime(2023, 11, 25),
    schedule_interval='0 */5 * * *',  # run this dag ever 5 hours
    catchup=False,  
) as dag:
    def push_to_xcom(**kwargs):
        # Push a variable to XCom
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='example_key', value='example_value')

        # Push multiple values to XCom by calling xcom_push multiple times
    
    def pull_multiple_values(**kwargs):
        task_instance = kwargs['ti']

    # Pull values from XCom
        values = task_instance.xcom_pull(task_ids='push_to_xcom', key='example_key', include_prior_dates=True)

        # 'include_prior_dates=True' includes values from prior dag runs if any
        # The 'values' variable will contain a list of values pushed to XCom

        # pulled values will be [e, x, a, m, p, l, e, _, v, a, l, u, e]
        for value in values:
            print(f'Pulled value: {value}')

task_push_xcom = PythonOperator(
    task_id='push_to_xcom',
    python_callable=push_to_xcom,
    provide_context=True,  # This is important for passing the task instance to the Python callable
    dag=dag,
)

# You can define additional tasks here if needed

# Define how to pull multiple values at once

task_pull_values = PythonOperator(
    task_id='pull_multiple_values',
    python_callable=pull_multiple_values,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
task_push_xcom >> task_pull_values
