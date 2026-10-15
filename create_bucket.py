from google.cloud import storage
from google.api_core.exceptions import NotFound, Conflict
import os
import random 

# Authentication
# This file must exist and contain valid service account credentials.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

random_number = random.randint(100, 999)
PROJECT_ID = "test-project-511411"
BUCKET_NAME = f"bkt-dev-{random_number}"
LOCATION = "ASIA-SOUTH2"  # Delhi region


def create_bucket():
    # Initialize the Cloud Storage client   
    storage_client = storage.Client(project=PROJECT_ID)

    # Check whether the bucket already exists
    bucket = storage_client.lookup_bucket(BUCKET_NAME)

    if bucket is not None:
        print(f"Bucket '{BUCKET_NAME}' already exists.")
        return

    try:
        # Create the bucket with the location specified upfront
        bucket = storage_client.create_bucket(
            BUCKET_NAME,
            project=PROJECT_ID,
            location=LOCATION,
        )

        print(
            f"Bucket '{bucket.name}' created successfully "
            f"in location '{bucket.location}'."
        )

    except Conflict:
        print(
            f"Bucket name '{BUCKET_NAME}' is already taken. "
            "Choose another globally unique name."
        )
    except Forbidden as error:
        print(f"Permission denied: {error}")
        print("Check your IAM permissions and Cloud Storage API.")

    except Exception as error:
        print(f"Error creating bucket: {error}")

if __name__ == "__main__":
    create_bucket()