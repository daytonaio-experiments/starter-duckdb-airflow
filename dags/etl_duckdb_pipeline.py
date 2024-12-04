from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import duckdb
from datetime import datetime

# Defining the DAG
dag = DAG(
    'etl_duckdb_pipeline',
    description='Simple ETL Pipeline with DuckDB',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2024, 11, 29),
    catchup=False,
)

# Step 1: Extracting Data from CSV
def extract_data():
    # Path to the raw CSV file
    input_file = '/workspace/starter-duckdb-airflow/data/products.csv'
    
    # Loading the CSV into a pandas DataFrame
    df = pd.read_csv(input_file)
    
    # Print the first few rows to verify
    print("Extracted Data:\n", df.head())
    
    return df

# Step 2: Transforming Data using DuckDB
def transform_data(**kwargs):
    # Pull the DataFrame from XCom (passing via kwargs)
    df = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    # Checking if data was retrieved
    if df is None:
        print("Error: Data not found in XCom.")
        return

    # Connecting to DuckDB (in-memory)
    con = duckdb.connect()

    # Register the dataframe with DuckDB
    con.register('products', df)

    # Example Transformation: Apply 10% discount to price
    transformed_df = con.execute("""
        SELECT id, product_name, price * 0.9 AS discounted_price
        FROM products
    """).df()  # Converting the result back into a pandas DataFrame
    
    # Printing the transformed data (can be removed later)
    print("Transformed Data:\n", transformed_df.head())
    
    return transformed_df

# Step 3: Loading Data back to CSV
def load_data(**kwargs):
    # Retrieving the transformed DataFrame from XCom
    df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    # Checking if data was retrieved
    if df is None:
        print("Error: Data not found in XCom.")
        return
    
    # Ensuring the DataFrame is loaded correctly
    if isinstance(df, pd.DataFrame):
        print("DataFrame received for saving:")
        print(df.head())  # Log the first few rows of the DataFrame
        
        # Save the DataFrame to another CSV file
        output_file = '/workspace/starter-duckdb-airflow/output/discounted_products.csv'
        df.to_csv(output_file, index=False)
        print(f"Transformed data saved to {output_file}")
    else:
        print(f"Error: Expected DataFrame but got: {type(df)}")

# Defining the Airflow tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Passing context for XCom pulling
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Passing context for XCom pulling
    dag=dag,
)

# Setting task dependencies (task order)
extract_task >> transform_task >> load_task