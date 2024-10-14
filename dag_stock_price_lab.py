# Code by Utkarsh Tripathi (017855552) and Leela Prasad (018210452)
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import timedelta, datetime
import pandas as pd
import requests
import snowflake.connector

# Function to get Snowflake connection
def return_snowflake_conn():
    user_id = Variable.get('Snowflake_UserName')
    password = Variable.get('Snowflake_Password')
    account = Variable.get('Snowflake_Account')
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='trip_db',
        schema='raw_data'
    )
    return conn

# Default arguments for the DAG
default_args = {
    'owner': 'Utkarsh Tripathi',
    'depends_on_past': False,
    'email_on_failure': ['tripathiutkarsh46@gmail.com'],
    'email_on_retry': ['tripathiutkarsh46@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='Stock_data_analysis_for_two_symbols',
    default_args=default_args,
    description='A DAG to fetch stock prices and insert into Snowflake',
    schedule='00 17 * * *',
    start_date=datetime(2024, 10, 10),
    catchup=False
) as dag:

    @task()
    def extract(symbols):
        """Extract the last 90 days of stock prices for multiple symbols and return a single DataFrame."""
        vantage_api_key = Variable.get('API_Key')
        all_data = []  # Initialize an empty list to store data for both symbols

        for symbol in symbols:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
            r = requests.get(url)
            data = r.json()

            # Retrieve time series data
            time_series = data.get("Time Series (Daily)", {})
            date_list = sorted(time_series.keys())[-90:]  # Get the last 90 days

            # Append data for each symbol
            for date in date_list:
                daily_data = time_series[date]
                all_data.append({
                    "date": date,
                    "open": float(daily_data["1. open"]),
                    "high": float(daily_data["2. high"]),
                    "low": float(daily_data["3. low"]),
                    "close": float(daily_data["4. close"]),
                    "volume": int(daily_data["5. volume"]),
                    "symbol": symbol
                })

        # Create a DataFrame from the combined data for all symbols
        df = pd.DataFrame(all_data)
        return df  # Return the combined DataFrame

    @task()
    def create_stock_prices_table():
        """Create the stock prices table in Snowflake."""
        conn = return_snowflake_conn()
        curr = conn.cursor()

        create_table_query = """
            CREATE OR REPLACE TABLE raw_data.stock_prices_symbols (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INTEGER,
                symbol VARCHAR
            );
        """
        try:
            curr.execute(create_table_query)
            print("Table created successfully.")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            curr.close()

    @task()
    def insert_stock_prices(df_data):
        """Insert stock data into the Snowflake table ensuring idempotency."""
        conn = return_snowflake_conn()
        curr = conn.cursor()

        df = pd.DataFrame(df_data)
        insert_query = """
        INSERT INTO raw_data.stock_prices_symbols (date, open, high, low, close, volume, symbol)
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM raw_data.stock_prices_symbols
            WHERE date = %s AND symbol = %s
        )
        """

        try:
            conn.cursor().execute("BEGIN")
            for index, row in df.iterrows():
                curr.execute(insert_query,
                             (row['date'], row['open'], row['high'], row['low'],
                              row['close'], row['volume'], row['symbol'],
                              row['date'], row['symbol']))
            conn.cursor().execute("COMMIT")
            print("Data inserted successfully.")
        except Exception as e:
            conn.cursor().execute("ROLLBACK")
            print(f"Error inserting data: {e}")
        finally:
            curr.close()

    @task()
    def check_table_stats():
        """Fetch and print the first row to verify data insertion."""
        conn = return_snowflake_conn()
        curr = conn.cursor()
        try:
            curr.execute("SELECT * FROM raw_data.stock_prices_symbols LIMIT 1")
            first_row = curr.fetchone()
            print(f"First row in the table: {first_row}")
        except Exception as e:
            print(f"Error fetching data: {e}")
        finally:
            curr.close()

    # Task dependencies
    symbols = ['AAPL', 'MSFT']  # You can set the symbol dynamically
    stock_data = extract(symbols)
    create_table = create_stock_prices_table()
    insert_data = insert_stock_prices(stock_data)
    check_stats = check_table_stats()

    create_table >> stock_data >> insert_data >> check_stats
