
import csv
import io
import logging
import os

import requests
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from google.cloud import bigquery, storage
import google.auth
import google.auth.transport.requests
from google.auth.exceptions import DefaultCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Configuration
PROJECT_ID = "test-project-511411"
REGION = "asia-south2"

BUCKET_NAME = "asia-south2-test-airflow-421f0ed1-bucket"

TEMP_LOCATION = f"gs://{BUCKET_NAME}/temp"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/staging"

BLOB_NAME = "api_data.csv"
SOURCE_PATH = f"gs://{BUCKET_NAME}/{BLOB_NAME}"

# Separate output prefix; never overwrite the source CSV.
INVALID_OUTPUT_PREFIX = (
    f"gs://{BUCKET_NAME}/invalid/api_data_invalid"
)

API_URL = "https://jsonplaceholder.typicode.com/posts"

EXPECTED_COLUMNS = ["userId", "id", "title", "body"]

DATASET_ID = "dataset001"
TABLE_ID = "api_data"

BIGQUERY_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

TABLE_SCHEMA = {
    "fields": [
        {"name": "userId", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "title", "type": "STRING", "mode": "REQUIRED"},
        {"name": "body", "type": "STRING", "mode": "REQUIRED"},
    ]
}

INVALID_COLUMNS = [
    "userId", "id", "title", "body", "error", "raw_record"
]

def verify_google_credentials():
    """Verify Google Cloud credentials in Cloud Composer."""
    try:
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        credentials.refresh(
            google.auth.transport.requests.Request()
        )

        logger.info(
            "ADC authentication successful. Project: %s",
            project,
        )

        return credentials

    except Exception as exc:
        raise RuntimeError(
            "Google Cloud authentication failed. Check the "
            "Composer service account and its permissions."
        ) from exc


def access_api():
    """Fetch API data and upload CSV to GCS."""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or not data:
        raise ValueError("API returned no valid records")

    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=EXPECTED_COLUMNS,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(data)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    bucket.blob(BLOB_NAME).upload_from_string(
        buffer.getvalue(),
        content_type="text/csv",
    )

    logger.info("Uploaded API data to %s", SOURCE_PATH)


def ensure_bigquery_dataset():
    """Create the BigQuery dataset if it does not exist."""
    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = REGION

    client.create_dataset(dataset, exists_ok=True)

    logger.info(
        "BigQuery dataset ready: %s in %s",
        dataset_ref,
        REGION,
    )


def parse_and_validate(line):
    """Validate a CSV record and classify it as valid or invalid."""
    try:
        values = next(csv.reader([line]))

        if len(values) != len(EXPECTED_COLUMNS):
            raise ValueError("Incorrect number of columns")

        row = dict(zip(EXPECTED_COLUMNS, values))

        user_id = row["userId"].strip()
        post_id = row["id"].strip()
        title = row["title"].strip()
        body = row["body"].strip()

        if not user_id.isdigit() or int(user_id) <= 0:
            raise ValueError("userId must be a positive integer")

        if not post_id.isdigit() or int(post_id) <= 0:
            raise ValueError("id must be a positive integer")

        if not title or not body:
            raise ValueError("title and body cannot be empty")

        return "valid", {
            "userId": int(user_id),
            "id": int(post_id),
            "title": title,
            "body": body,
        }

    except (ValueError, TypeError, csv.Error) as exc:
        # Use strings in the invalid CSV, not BigQuery INTEGER values.
        return "invalid", {
            "userId": "",
            "id": "",
            "title": "",
            "body": "",
            "error": str(exc),
            "raw_record": line,
        }


def format_invalid_csv(record):
    """Convert an invalid record to a properly escaped CSV row."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=INVALID_COLUMNS,
        extrasaction="ignore",
    )
    writer.writerow(record)
    return buffer.getvalue().rstrip("\r\n")


def run_pipeline():
    verify_google_credentials()

    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        job_name="csv-to-bigquery-job",
        save_main_session=True,
    )

    with beam.Pipeline(options=options) as pipeline:
        results = (
            pipeline
            | "Read source CSV" >> beam.io.ReadFromText(
                SOURCE_PATH,
                skip_header_lines=1,
            )
            | "Parse and validate" >> beam.Map(parse_and_validate)
        )

        valid_records = (
            results
            | "Filter valid records" >> beam.Filter(
                lambda item: item[0] == "valid"
            )
            | "Extract valid rows" >> beam.Map(lambda item: item[1])
        )

        invalid_records = (
            results
            | "Filter invalid records" >> beam.Filter(
                lambda item: item[0] == "invalid"
            )
            | "Extract invalid rows" >> beam.Map(lambda item: item[1])
        )

        # Write valid rows to BigQuery.
        _ = (
            valid_records
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=TABLE_SCHEMA,
                create_disposition=(
                    BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(
                    BigQueryDisposition.WRITE_APPEND
                ),
                custom_gcs_temp_location=TEMP_LOCATION,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            )
        )

        # Write invalid rows to a separate GCS output.
        _ = (
            invalid_records
            | "Format invalid CSV rows" >> beam.Map(
                format_invalid_csv
            )
            | "Write invalid CSV to GCS" >> beam.io.WriteToText(
                INVALID_OUTPUT_PREFIX,
                file_name_suffix=".csv",
                header=",".join(INVALID_COLUMNS),
                num_shards=1,
            )
        )


if __name__ == "__main__":
    ensure_bigquery_dataset()
    access_api()
    run_pipeline()
