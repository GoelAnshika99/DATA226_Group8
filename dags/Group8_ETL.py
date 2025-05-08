from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from snowflake.connector.cursor import SnowflakeCursor

import yfinance as yf

database = Variable.get('snowflake_database')
warehouse = Variable.get('snowflake_warehouse')
symbol = Variable.get('stock_symbol')

def get_snowflake_connection() -> SnowflakeCursor:
    '''
    Return a cursor that is connected to Snowflake
    '''
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

def get_logical_date() -> datetime:
    '''
    Get the current logical date
    '''
    context = get_current_context()
    if 'logical_date' in context:
        return context['logical_date']
    return datetime.today() - timedelta(days=1)

def populate_table_via_stage(cur: SnowflakeCursor, table_name: str, file_path: str):
    """
    Populate a table with data from a given CSV file using Snowflake's COPY INTO command.
    """
    stage_name = f"TEMP_STAGE_{table_name}"
    file_name = os.path.basename(file_path)

    # First set the schema since table stage or temp stage needs to have the schema as the target table
    cur.execute(f"USE SCHEMA {database}.raw")

    # Create a temporary named stage
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name}")

    # Copy the given file to the temporary stage
    cur.execute(f"PUT file://{file_path} @{stage_name}")

    # Run copy into command with fully qualified table name
    copy_query = f"""
        COPY INTO raw.{table_name}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)

@task
def build_data_warehouse(cur: SnowflakeCursor):
    '''
    Creates the warehouse, database, and schemas in Snowflake.
    '''
    try:
        cur.execute('BEGIN;')
        # Create warehouse
        logging.info(f'Creating warehouse {warehouse}')
        cur.execute(f'''CREATE WAREHOUSE IF NOT EXISTS {warehouse}
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
        cur.execute('CREATE SCHEMA IF NOT EXISTS raw;')

        logging.info(f'Creating schema curation')
        cur.execute('CREATE SCHEMA IF NOT EXISTS curation;')

        logging.info(f'Creating schema analytics')
        cur.execute('CREATE SCHEMA IF NOT EXISTS analytics;')
        cur.execute('COMMIT;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        logging.error(e)
        raise e

@task
def extract_transform_historical_data(infile_path: str, outfile_path: str):
    historical = pd.read_csv(infile_path)

    historical = historical[historical.index > 1].reset_index(drop=True)

    cols = list(historical.columns)
    cols[0] = 'Date'
    historical.columns = cols

    historical = historical.drop('Adj Close', axis='columns')
    historical['Symbol'] = symbol

    historical.to_csv(outfile_path, index=False)

@task
def load_historical_data(cur: SnowflakeCursor, file_path: str):
    try:
        cur.execute('BEGIN;')

        populate_table_via_stage(cur, f'{symbol}_stock', file_path)

        cur.execute('COMMIT;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        logging.error(e)
        raise e

@task
def extract(symbol: str) -> str:
    current_date = get_logical_date()
    next_date = current_date + timedelta(days=1)

    data = yf.download([symbol], start=current_date, end=next_date, multi_level_index=False)
    if data is None:
        raise ValueError

    data['Symbol'] = symbol

    filename = f'/tmp/{symbol}_{str(current_date.date())}.csv'
    data.to_csv(filename)
    return filename

@task
def load(cur: SnowflakeCursor, data_file: str, table_name: str):
    current_date = get_logical_date()
    try:
        cur.execute('BEGIN;')

        # Create the table if it doesn't already exist
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {database}.raw.{table_name} (
            date DATE,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume INT,
            symbol VARCHAR
        );''')

        cur.execute(f"DELETE FROM {database}.raw.{table_name} WHERE date='{str(current_date.date())}'")
        populate_table_via_stage(cur, table_name, data_file)

        cur.execute('COMMIT;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        logging.error(e)
        raise e

with DAG(
    dag_id='Group_ETL',
    start_date=datetime(2024, 10, 1),
    catchup=True,
    tags=['lab'],
    schedule='0 2 * * *'
) as dag:
    cur = get_snowflake_connection()

    build_dw_instance = build_data_warehouse(cur)

    # extract_instance is used both to order the tasks and pass into load as a parameter
    # Creating a duplicate variable stocks_file highlights when each is used
    extract_instance = extract(symbol)
    stocks_file = extract_instance

    load_instance = load(cur, stocks_file, f'{symbol}_stock')

    # This forces the data warehouse to be built and the data to be extracted before we start loading into the data warehouse
    [build_dw_instance, extract_instance] >> load_instance

with DAG(
    dag_id='Load_Historical_Data',
    start_date=datetime(2018, 1, 2),
    catchup=False,
    tags=['etl'],
    schedule='0 2 * * *'
) as dag2:
    cur = get_snowflake_connection()

    build_dw_instance = build_data_warehouse(cur)

    temp_file = '/tmp/historical_transformed.csv'

    et_instance = extract_transform_historical_data('/opt/airflow/dags/NVIDIA_STOCK.csv', temp_file)

    load_instance = load_historical_data(cur, temp_file)

    [build_dw_instance, et_instance] >> load_instance
