import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import os
from google.cloud import storage, bigquery
from google.cloud.exceptions import Notfound

BIGQUERY_TABLE = "test-project-511411:dataset001.sample"
PROJECT_ID="test-project-511411"
REGION="asia-south2"
TEMP_LOCATION="gs://bkt-dev-737/temp"
STAGING_LOCATION="gs://bkt-dev-737/staging"
INPUT_FILE="gs://bkt-dev-737/sample.csv"

# BigQuery schema
TABLE_SCHEMA = {
    "fields": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    ]
}

#Dataflow Configurations
options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID,
    region=REGION,
    temp_location=TEMP_LOCATION,
    staging_location=STAGING_LOCATION,
    job_name="csv-to-bigquery-job",
    save_main_session=True,
)

# Function to parse csv
def parse_line(line):
    if not line.strip():
        return None

    fields = next(csv.reader([line]))
    
    # Skip the CSV header
    if fields[0].strip().lower() == "id":
        return None

    if len(fields) < 2:
        raise ValueError(f"Invalid CSV row: {line}")

    return {
        'id': int(fields[0]),
        'name': fields[1]
    }

def run_pipeline():
    with beam.Pipeline(options=options) as pipeline:
        (pipeline
            | 'Read CSV file from GCS' 
            >> beam.io.ReadFromText(INPUT_FILE)
            | 'Parse CSV' 
            >> beam.Map(parse_line)
            | "Remove headers and empty rows"
            >> beam.Filter(lambda row: row is not None)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=TABLE_SCHEMA,
                create_disposition=(
                    BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(BigQueryDisposition.WRITE_TRUNCATE)
            )
        )

if __name__ == "__main__":
    run_pipeline()