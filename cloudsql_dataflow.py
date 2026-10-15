import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud.sql.connector import Connector, IPTypes
from google.cloud import secretmanager
import pymysql

# Authentication
# This file must exist and contain valid service account credentials.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"


PROJECT_ID = "test-project-511411"
REGION = "asia-south2"
INSTANCE_CONNECTION_NAME = "test-project-511411:asia-south2:test-instance"

DB_NAME = "beam-demo"
DB_USER = "admin"
TABLE_NAME = "employees"
BATCH_SIZE = 100

# Secret Manager secret ID containing the database password.
DB_PASSWORD_SECRET_ID = "cloudsql-db-password"

TEMP_LOCATION = "gs://bkt-dev-737/dataflow-temp"
STAGING_LOCATION = "gs://bkt-dev-737/dataflow-staging"


def read_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


class WriteToCloudSQL(beam.DoFn):
    def __init__(
        self,
        instance_connection_name,
        db_name,
        db_user,
        password_secret_id,
        project_id,
        batch_size=100,
        use_private_ip=True,
    ):
        self.instance_connection_name = instance_connection_name
        self.db_name = db_name
        self.db_user = db_user
        self.password_secret_id = password_secret_id
        self.project_id = project_id
        self.batch_size = batch_size
        self.use_private_ip = use_private_ip
        self.rows = []
        self.connector = None
        self.db_password = None

    def setup(self):
        # Setup is called on each worker instance.
        #self.db_password = read_secret(self.project_id, self.password_secret_id)
        self.connector = Connector()

    def process(self, record):
        self.rows.append(record)
        if len(self.rows) >= self.batch_size:
            self.flush()       
        return

    def flush(self):
        if not self.rows:
            return

        # Copy the current batch so a failed transaction does not silently
        # discard it. If this raises, Dataflow may retry the bundle.
        batch = list(self.rows)
        connection = None

        try:
            ip_type = IPTypes.PRIVATE if self.use_private_ip else IPTypes.PUBLIC
            connection = self.connector.connect(
                self.instance_connection_name,
                "pymysql",
                user=self.db_user,
                password=self.db_password,
                db=self.db_name,
                ip_type=ip_type,
            )

            sql = f"""
                INSERT INTO `{self.db_name}`.`{self.db_name}`
                    (id, name, email, department)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    email = VALUES(email),
                    department = VALUES(department)
            """
            # Correct table qualification is schema/table, not database/database.
            sql = """
                INSERT INTO employees (id, name, email, department)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    email = VALUES(email),
                    department = VALUES(department)
            """
            with connection.cursor() as cursor:
                cursor.executemany(sql, batch)
            connection.commit()

            # Clear only after the database transaction succeeds.
            del self.rows[:len(batch)]
            logging.info("Successfully upserted %s employee rows", len(batch))

        except Exception:
            if connection:
                connection.rollback()
            logging.exception("Failed to write batch to Cloud SQL")
            raise
        finally:
            if connection:
                connection.close()

    def finish_bundle(self):
        self.flush()

    def teardown(self):
        if self.connector:
            self.connector.close()


def run(argv=None):
    # On Google Cloud, prefer the attached worker service account / ADC.
    # For local testing only, you can set GOOGLE_APPLICATION_CREDENTIALS
    # in your shell to the path of a service-account JSON key.
    options = PipelineOptions(
        argv,
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        job_name="beam-cloudsql-insert",
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        save_main_session=True,
        requirements_file="requirements.txt",
    )
    options.view_as(SetupOptions).save_main_session = True

    employees = [
        (1, "Amit Sharma", "amit@example.com", "Engineering"),
        (2, "Priya Singh", "priya@example.com", "HR"),
        (3, "Rahul Verma", "rahul@example.com", "Finance"),
        (4, "Neha Gupta", "neha@example.com", "Engineering"),
        (5, "Vikram Rao", "vikram@example.com", "Marketing"),
    ]

    with beam.Pipeline(options=options) as pipeline:
        _ = (
            pipeline
            | "Create employee records" >> beam.Create(employees)
            | "Upsert records into Cloud SQL" >> beam.ParDo(
                WriteToCloudSQL(
                    instance_connection_name=INSTANCE_CONNECTION_NAME,
                    db_name=DB_NAME,
                    db_user=DB_USER,
                    password_secret_id=DB_PASSWORD_SECRET_ID,
                    project_id=PROJECT_ID,
                    batch_size=BATCH_SIZE,
                    use_private_ip=True,
                )
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()