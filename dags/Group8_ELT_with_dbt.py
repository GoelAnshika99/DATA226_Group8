from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt_group"


conn = BaseHook.get_connection('snowflake_conn_mallard')
with DAG(
    "Group_ELT_dbt",
    start_date=datetime(2025, 1, 1),
    description="An Airflow DAG to invoke dbt runs using a BashOperator",
    schedule="30 2 * * *",   # 02:30 daily, after the 02:00 ETL jobs
    catchup=False,
    tags=['elt'],
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:

    wait_daily_etl = ExternalTaskSensor(
        task_id="wait_daily_etl",
        external_dag_id="ETL_Daily_Data",
        external_task_id="load",  # final task in ETL_Daily_Data
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=300,           # every 5 min
        timeout=60 * 60 * 2,        # give up after 2 h
        mode="reschedule",
    )
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    # print_env_var = BashOperator(
    #    task_id='print_aa_variable',
    #    bash_command='echo "The value of AA is: $DBT_ACCOUNT,$DBT_ROLE,$DBT_DATABASE,$DBT_WAREHOUSE,$DBT_USER,$DBT_TYPE,$DBT_SCHEMA"'
    # )

    dbt_run >> dbt_test >> dbt_snapshot
