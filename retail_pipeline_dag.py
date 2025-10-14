from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

import requests
import zipfile
import io

import pandera.pandas as pa
import os
from google.cloud import bigquery # Keep this for BQ connection

# --- Global File Names ---
RAW_FILE = 'online_retail.csv'
CLEAN_FILE = 'cleaned_retail.csv'
VALIDATED_FILE = 'validated_retail.csv'

# --- Extract Data ----
def extract_data():
    """Downloads the data, extracts the Excel file, and saves it as a CSV."""
    url = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        with zf.open('Online Retail.xlsx') as f:
            # Note: We hardcode dtype={'InvoiceNo': str} as it was in the original
            df = pd.read_excel(f, dtype={'InvoiceNo': str})

    # Saves to the globally defined raw file name
    df.to_csv(RAW_FILE, index=False)
    # The return value is technically not needed without XCom, but can be kept for logging
    return RAW_FILE

# --- Cleansing Data ---
# It now directly uses the hardcoded RAW_FILE name
def clean_data():
    """Performs data cleaning and saves the result to the clean file."""
    # Load the DataFrame from the hardcoded raw file path
    df = pd.read_csv(RAW_FILE)

    # Remove rows with missing CustomerID (as it's key for behavior analysis)
    df = df.dropna(subset=["CustomerID"])

    # Remove cancelled invoices (InvoiceNo start with 'C')
    df = df[~df['InvoiceNo'].str.startswith('C', na=False)]

    # Remove invalid quantities and price
    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]

    # Convert data types
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    # Convert CustomerID to int only after dropping NaNs
    df['CustomerID'] = df['CustomerID'].astype(int)

    # Add total price column for analysis
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    # Remove duplicates
    df = df.drop_duplicates()

    # Save to the globally defined clean file name
    df.to_csv(CLEAN_FILE, index=False)

    return CLEAN_FILE


# --- Validate Data ---
# It now directly uses the hardcoded CLEAN_FILE name
def validate_data():
    """Validates the cleaned data against a schema and saves the validated result."""
    # Load the DataFrame from the hardcoded clean file path
    # df = pd.read_csv(CLEAN_FILE, dtype={'InvoiceNo':str})
    df = pd.read_csv(CLEAN_FILE, dtype={'InvoiceNo': str}, parse_dates=['InvoiceDate'])

    schema = pa.DataFrameSchema({
        'InvoiceNo': pa.Column(pa.String, checks=pa.Check.str_length(min_value=6, max_value=7)),
        'StockCode': pa.Column(pa.String),
        'Description': pa.Column(pa.String, nullable=True),
        'Quantity': pa.Column(pa.Int, checks=pa.Check.gt(0)),
        'InvoiceDate': pa.Column(pa.DateTime),
        'UnitPrice': pa.Column(pa.Float, checks=pa.Check.gt(0)),
        'CustomerID': pa.Column(pa.Int),
        'Country': pa.Column(pa.String),
        'TotalPrice': pa.Column(pa.Float, checks=pa.Check.gt(0))
    })

    try:
        validated_df = schema.validate(df)
        print("Validation Pass!")
        # Save to the globally defined validated file name
        validated_df.to_csv(VALIDATED_FILE, index=False)

        return VALIDATED_FILE

    except pa.errors.SchemaErrors as e:
        # Airflow will mark the task as failed if an exception is raised
        raise ValueError(f"Validation fail: {e}")

# --- Load Data ---
# It now directly uses the hardcoded VALIDATED_FILE name
# **FIXED BUG**: changed `_name` to `f_name` in pd.read_csv
def load_to_bigquery(project_id, dataset_id='retail_analysis', table_id='transactions'):
    """Loads the validated data into Google BigQuery."""
    # Load the validated DataFrame just before loading to BQ
    # Use the globally defined validated file name
    df = pd.read_csv(VALIDATED_FILE)

    # Set credential (NOTE: This should be configured via Airflow Secrets/Connections in production)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/mulyo/Learning/bigquery/psychic-mason-473812-u3-ae4add80cbb0.json"

    client = bigquery.Client(project=project_id)

    # Create dataset if not exist
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    # The create_dataset method needs the dataset object, not the reference.
    # We use exists_ok=True to prevent errors if it already exists.
    dataset = client.create_dataset(dataset, exists_ok=True)

    # Load data
    tabel_ref = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig(
        # Set to overwrite the table each run
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True # Autodetect schema from the DataFrame
    )
    job = client.load_table_from_dataframe(df, tabel_ref, job_config=job_config)
    job.result() # Wait for job complete
    print(f"Loaded {job.output_rows} rows to {dataset_id}.{table_id}")


# --- Airflow DAG Definition ---
with DAG(
    'retail_data_pipeline_no_xcom', # Renamed DAG ID
    description='ETL pipeline for online retail data (without XCom)',
    schedule='@daily',
    start_date=datetime(2025, 10, 11),
    catchup=False,
    tags=['BigQuery', 'Simplified']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        # Removed provide_context=True
    )

    clean_task = PythonOperator(
        task_id='clean_data', # Renamed task_id for clarity
        python_callable=clean_data,
        # Removed op_kwargs for XCom
    )

    validate_task = PythonOperator(
        task_id='validate_data', # Renamed task_id for clarity
        python_callable=validate_data,
        # Removed op_kwargs for XCom
    )

    load_task = PythonOperator(
        task_id='load_to_bigquery', # Renamed task_id for clarity
        python_callable=load_to_bigquery,
        op_kwargs={
            # Passed project_id as a required parameter
            'project_id': 'psychic-mason-473812-u3'
        },
    )

    # Define task dependencies
    extract_task >> clean_task >> validate_task >> load_task
