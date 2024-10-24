from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging


def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn


@task
def create_joined_table(target_table1, target_table2, target_joined_table):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    try:
        # Create the joined table
        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_joined_table} AS
            SELECT t1.*, t2.ts
            FROM {target_table1} t1
            JOIN {target_table2} t2 ON t1.sessionId = t2.sessionId;
        """)

        logging.info(f"Table {target_joined_table} created successfully.")

        # Checking for Primary key uniqueness
        cur.execute(f"""
            SELECT sessionId, COUNT(1) AS cnt 
            FROM {target_joined_table} 
            GROUP BY sessionId 
            HAVING COUNT(1) > 1;
        """)
        #Checking for Duplicate
        duplicates = cur.fetchall()
        if duplicates:
            logging.error(f"Primary key uniqueness violation: {duplicates}")
            raise Exception(f"Primary key uniqueness failed. Duplicates found: {duplicates}")
        else:
            logging.info("Primary key uniqueness check passed. No duplicates found.")

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id='SessionSummaryWithConstraintsCheck',
    start_date=datetime(2024, 10, 23),
    catchup=False,
    schedule_interval='00 6 * * *',
    tags=['ELT'],
) as dag:
    tgt_table1 = "dev.raw_data.user_session_channel"
    tgt_table2 = "dev.raw_data.session_timestamp"
    joined_table = "dev.analytics.session_summary"

    
    create_joined_task = create_joined_table(tgt_table1, tgt_table2, joined_table)

    # Task Trigger
    create_joined_task
