from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gender_prediction_dag',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
)

def parse_and_load_to_postgres(**kwargs):
    # Parse the JSON result from the XCom
    prediction_results = kwargs['ti'].xcom_pull(task_ids='predict_gender_task')
    
    # Define the table schema
    table_schema = """
        CREATE TABLE IF NOT EXISTS gender_name_prediction (
            input JSON,
            details JSON,
            result_found BOOLEAN,
            first_name VARCHAR(255),
            probability FLOAT,
            gender VARCHAR(50),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create the table
    cursor.execute(table_schema)
    conn.commit()
    result = json.loads(prediction_results)

    # Insert data into the table
    # for result in prediction_results:
    #     if isinstance(result, str):
    #         # Handle case where result is a string
    #         result = json.loads(result)
    cursor.execute("""
        INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        json.dumps(result.get("input")),
        json.dumps(result.get("details")),
        result.get("result_found"),
        result.get("first_name"),
        result.get("probability"),
        result.get("gender"),
    ))
    
    conn.commit()
    conn.close()
    
predict_gender_task = SimpleHttpOperator(
    task_id='predict_gender_task',
    method='POST',
    endpoint="/gender/by-first-name-multiple",
    data= [{"first_name":"Thomas","country":"US"},{"first_name":"Bianca","country":"US"}]
    # [
    #         {"first_name":"Sandra","country":"US"},
    #         {"first_name":"Jason","country":"US"},
    #         {"first_name":"Juman","country":"US"},
    #         {"first_name":"Ado","country":"US"},
    #         {"first_name":"James","country":"US"},
    #         {"first_name":"John","country":"US"},
    #         {"first_name":"Jack","country":"US"},
    #         {"first_name":"Tyler","country":"US"},
    #         {"first_name":"Jessica","country":"US"},
    #         {"first_name":"Bianca","country":"US"},
    #         {"first_name":"Thomas","country":"US"},
    #         {"first_name":"Ruairi","country":"US"},
    #         ],
    http_conn_id="gender_api",
    log_response=True,
    dag=dag
    )

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=parse_and_load_to_postgres,
    provide_context=True,
    dag=dag,
)

predict_gender_task >> create_table_task
