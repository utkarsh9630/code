#Train and Predict DAG code by Utkarsh Tripathi (017855552) and Leela Prasad (018210452)
# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    user_id = Variable.get('Snowflake_UserName')
    password = Variable.get('Snowflake_Password')
    account = Variable.get('Snowflake_Account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='compute_wh',
        database='trip_db'
    )
    # Create a cursor object
    return conn.cursor()


@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        CAST(DATE AS TIMESTAMP_NTZ) AS DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


with DAG(
    dag_id = 'TrainandPredict',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '10 17 * * *'
) as dag:

    train_input_table = "trip_db.raw_data.stock_prices_symbols"
    train_view = "trip_db.raw_data.stock_prices_data_DAG"
    forecast_table = "trip_db.adhoc.stock_forecast_table"
    forecast_function_name = "trip_db.analytics.stock_forecast_model"
    final_table = "trip_db.analytics.final_stock_predictions"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)