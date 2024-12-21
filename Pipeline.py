import pandas as pd
from google.cloud import storage
from io import StringIO
from google.oauth2 import service_account
import pyodbc
from google.cloud import bigquery


key_path = 'C:/Users/IDX-161/Documents/project-ishar-eea19e90f538.json'

credentials = service_account.Credentials.from_service_account_file(key_path)

def read_csv_from_gcs(bucket_name, blob_name):
    """
    Reads a CSV file from Google Cloud Storage and loads it into a Pandas DataFrame.
    :param bucket_name: Name of the GCS bucket.
    :param blob_name: Name of the blob (file) in the bucket.
    :return: Pandas DataFrame containing the CSV data.
    """
    try: 
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        csv_content = blob.download_as_text()
        csv_file = StringIO(csv_content)
        df = pd.read_csv(csv_file)
        print("CSV file read successfully from GCS")
        return df
    except Exception as e:
        print("Error reading CSV from GCS:", e)
        return None

def read_from_azure_sql(server, database, username, password, query):
    """
    Reads data from an Azure SQL database and loads it into a Pandas DataFrame.
    :param server: Azure SQL Server name.
    :param database: Database name.
    :param username: Username for the database.
    :param password: Password for the database.
    :param query: SQL query to execute.
    :return: Pandas DataFrame containing the query result.
    """
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        conn = pyodbc.connect(conn_str)
        print("Connected to Azure SQL Server")
        df = pd.read_sql(query, conn)
        print("Query executed successfully")
        return df
    except Exception as e:
        print("Error executing query:", e)
        return None
    finally:
        conn.close()
        print("Connection closed")

def upload_to_bigquery(merged_df, project_id, dataset_id, table_id):
    """
    Uploads a Pandas DataFrame to a BigQuery table.

    :param merged_df: DataFrame to upload.
    :param project_id: Google Cloud Project ID.
    :param dataset_id: BigQuery dataset name.
    :param table_id: BigQuery table name.
    """
    try:
        client = bigquery.Client.from_service_account_json(key_path)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(merged_df, table_ref)
        job.result()
        print(f"Table {table_id} uploaded to BigQuery in dataset {dataset_id}.")
    except Exception as e:
        print("Error uploading to BigQuery:", e)

if __name__ == "__main__":
    gcs_bucket_name = "buckettrainingtest"
    gcs_blob_name = "training_history_data.csv"
    print("Reading CSV from GCS...")
    gcs_df = read_csv_from_gcs(gcs_bucket_name, gcs_blob_name)
    if gcs_df is not None:
        print("Data from GCS:")
        print(gcs_df.head())

    azure_server = "isharsqldatabasedemo.database.windows.net"
    azure_database = "SampleDB"
    azure_username = "Ishar"
    azure_password = "Akusokuzan190788!"
    azure_query = "SELECT TOP 10 * FROM dbo.EmployeeData"  

    print("\nReading data from Azure SQL Server...")
    azure_df = read_from_azure_sql(azure_server, azure_database, azure_username, azure_password, azure_query)

    if azure_df is not None:
        print("Data from Azure SQL Server:")
        print(azure_df)

    if gcs_df is not None and azure_df is not None:
        try:
            merged_df = pd.merge(gcs_df, azure_df, on="EmployeeID", how="inner")
            print("Merged DataFrame:")
            print(merged_df)
            print("\nUploading merged data to BigQuery...")
            project_id = "project-ishar"
            dataset_id = "Test_Dataset"
            table_id = "CompileTable"
            upload_to_bigquery(merged_df, project_id, dataset_id, table_id)
        except Exception as e:
            print(f"Error during merging: {e}")
