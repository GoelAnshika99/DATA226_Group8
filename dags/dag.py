from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.cursor import SnowflakeCursor


database = Variable.get('snowflake_database')
warehouse = Variable.get('snowflake_warehouse')

def get_snowflake_connection() -> SnowflakeCursor:
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def build_data_warehouse(cur: SnowflakeCursor):
    '''
    Creates the warehouse, database, and schemas in Snowflake.
    '''
    try:
        cur.execute('BEGIN;')
        # Create warehouse
        logging.info(f'Creating warehouse {warehouse}')
        cur.execute(f'''CREATE IF NOT EXISTS WAREHOUSE {warehouse}
        WITH 
            WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 300
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED = TRUE;
        ''')
        
        # Create database
        logging.info(f'Creating database {database}')
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {database};')
        cur.execute(f'USE DATABASE {database};')

        # Create schemas
        logging.info(f'Creating schema raw')
        cur.execute('CREATE IF NOT EXISTS SCHEMA raw;')

        logging.info(f'Creating schema curation')
        cur.execute('CREATE IF NOT EXISTS SCHEMA curation;')

        logging.info(f'Creating schema analytics')
        cur.execute('CREATE IF NOT EXISTS SCHEMA analytics;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        logging.error(e)
        raise e

with DAG(
    dag_id='data_226_group_8',
    start_date=datetime(2025, 4, 20),
    catchup=False,
    tags=['lab'],
    schedule='0 0 * * *'
) as dag:
    cur = get_snowflake_connection()

    build_data_warehouse(cur)
