import os
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Authentication
# This file must exist and contain valid service account credentials.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

# Initialize Google Cloud Clients
storage_client = storage.Client()
bq_client = bigquery.Client(project="test-project-511411")

# Define the Cloud Storage bucket and BigQuery dataset
BUCKET_NAME='bkt-dev-737'
DATASET_NAME="dataset001"
TABLE_NAME="hr_database"

def load_csv_bigQuery(event, context=None):
    """
    Triggered by a change to a Cloud Storage bucket.
    This function loads a CSV file into BigQuery.
    """
    # Get the Cloud Storage file details    
    bucket_name = event.get('bucket')
    file_name = event.get('name')

    # Check if the file is in the expected bucket
    if bucket_name != BUCKET_NAME:
        print(f"File uploaded to an unexpected bucket: {bucket_name}. Expected {BUCKET_NAME}.")
        return

    print(f"File {file_name} uploaded to bucket {bucket_name}. Loading to BigQuery...")

    # Process only CSV files
    if not file_name.lower().endswith(".csv"):
        print(f"Skipping non-CSV file: {file_name}")
        return

    print(
        f"File {file_name} uploaded to bucket "
        f"{bucket_name}. Loading to BigQuery..."
    )

    # Cloud Storage URI
    uri = f'gs://{bucket_name}/{file_name}'

    # Configure the BigQuery load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present in the CSV
        autodetect=True,  # Automatically detect schema
    )
   
    # Define the destination table
    table_ref = bq_client.dataset(DATASET_NAME).table(TABLE_NAME)

   
    # Start the load job
    load_job = bq_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )
       
    # Wait for the load job to complete
    load_job.result()
    print(f"File {file_name} successfully loaded into BigQuery table {TABLE_NAME}.")

if __name__ == "__main__":
  
    # Local testing: replace with your actual CSV filename.
    test_event = {
        "bucket": BUCKET_NAME,
        "name": "yelp_database.csv",
    }

    load_csv_bigQuery(test_event)