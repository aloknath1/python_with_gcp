
import apache_beam as beam
import csv
import os

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition


# Google Cloud configuration
PROJECT_ID = "test-project-511411"
REGION = "asia-south2"
BUCKET = "bkt-dev-737"

INPUT_FILE = f"gs://{BUCKET}/sample.csv"
OUTPUT_TABLE = f"{PROJECT_ID}:dataset001.sample"

TEMP_LOCATION = f"gs://{BUCKET}/temp"
STAGING_LOCATION = f"gs://{BUCKET}/staging"

# Local authentication for development.
# Ensure key.json exists in your current working directory.
if os.path.exists("key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("key.json")


def parse_csv(line):
    try:
        row = next(csv.reader([line]))

        if len(row) < 2:
            raise ValueError(f"Expected at least 2 columns: {line}")

        return {
            "id": int(row[0].strip()),
            "name": row[1].strip()
        }

    except (ValueError, csv.Error, StopIteration) as exc:
        raise ValueError(
            f"Unable to parse CSV row {line!r}: {exc}"
        ) from exc


def run():
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        job_name="sqlfile-to-bigquery",
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                INPUT_FILE,
                skip_header_lines=1
            )
            | "Parse CSV Data" >> beam.Map(parse_csv)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=OUTPUT_TABLE,
                schema="id:INTEGER,name:STRING",
                create_disposition=(
                    BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(
                    BigQueryDisposition.WRITE_TRUNCATE
                ),
                custom_gcs_temp_location=TEMP_LOCATION
            )
        )


if __name__ == "__main__":
    run()
