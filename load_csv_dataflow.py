import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.util import BatchElements


PROJECT_ID = "test-project-511411"
REGION = "asia-south2"
BUCKET_NAME = "bkt-dev-737"
DATASET_NAME = "dataset001"
TABLE_NAME = "hr_database"
DEFAULT_INPUT = "gs://bkt-dev-737/HRA_Records.csv"


class ParseCsvLine(beam.DoFn):
    """Parse CSV rows using Python's CSV parser; skip the header."""
    def __init__(self, expected_columns=None):
        self.expected_columns = expected_columns

    def process(self, line):
        try:
            values = next(csv.reader([line]))
            if not values:
                return
            # Header row: skip if first field looks like a column heading.
            if values[0].strip().lower() in {"id", "employee_id", "business_id"}:
                return
            if self.expected_columns and len(values) != self.expected_columns:
                logging.warning(
                    "Skipping malformed CSV row: expected %s columns, got %s",
                    self.expected_columns, len(values)
                )
                return
            yield values
        except (csv.Error, StopIteration) as exc:
            logging.warning("Skipping invalid CSV line: %s", exc)


class RowsToJson(beam.DoFn):
    """Convert positional rows to JSON objects for BigQuery JSON writes."""
    def __init__(self, columns):
        self.columns = columns

    def process(self, values):
        if len(values) != len(self.columns):
            logging.warning("Skipping row with %s values; expected %s",
                            len(values), len(self.columns))
            return
        yield {name: value for name, value in zip(self.columns, values)}


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Cloud Storage CSV URI, e.g. gs://bkt-dev-737/yelp_database.csv",
    )
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--bucket", default=BUCKET_NAME)
    parser.add_argument("--dataset", default=DATASET_NAME)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--chunk_size", type=int, default=1000)
    parser.add_argument(
        "--columns",
        required=True,
        help="Comma-separated CSV column names in the same order as the CSV, "
             "excluding the header. Example: id,name,city",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="BigQuery schema. Example: id:INTEGER,name:STRING,city:STRING",
    )
    parser.add_argument("--temp_location",
                        default="gs://bkt-dev-737/dataflow-temp")
    parser.add_argument("--staging_location",
                        default="gs://bkt-dev-737/dataflow-staging")
    parser.add_argument("--job_name", default="csv-to-bigquery-chunked")
    known, pipeline_args = parser.parse_known_args(argv)

    if known.chunk_size < 1:
        parser.error("--chunk_size must be greater than zero")

    columns = [c.strip() for c in known.columns.split(",") if c.strip()]
    if not columns:
        parser.error("--columns must contain at least one column")

    options = PipelineOptions(
        pipeline_args,
        runner="DataflowRunner",
        project=known.project,
        region=known.region,
        temp_location=known.temp_location,
        staging_location=known.staging_location,
        job_name=known.job_name,
        save_main_session=True,
        requirements_file="requirements.txt",
    )
    options.view_as(SetupOptions).save_main_session = True

    table_spec = f"{known.project}:{known.dataset}.{known.table}"

    with beam.Pipeline(options=options) as pipeline:
        rows = (
            pipeline
            | "Read CSV from GCS" >> beam.io.ReadFromText(
                known.input, skip_header_lines=1
            )
            | "Parse CSV" >> beam.ParDo(ParseCsvLine(len(columns)))
        )

        # Chunk records in groups of up to 1,000. The batch is flattened back
        # into rows before BigQuery's scalable file-load sink.
        chunked_rows = (
            rows
            | "Batch 1000 rows" >> BatchElements(
                min_batch_size=1,
                max_batch_size=known.chunk_size,
            )
            | "Flatten batches" >> beam.FlatMap(lambda batch: batch)
        )

        (
            chunked_rows
            | "Map columns to JSON rows" >> beam.ParDo(RowsToJson(columns))
            | "Write BigQuery" >> beam.io.WriteToBigQuery(
                table=table_spec,
                schema=known.schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=known.temp_location,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
