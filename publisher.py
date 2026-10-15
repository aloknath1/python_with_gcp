import os
import time
import requests

from google.cloud import pubsub_v1
from google.api_core import retry, exceptions

# Authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

# API and Pub/Sub configuration
api = "https://jsonplaceholder.typicode.com/posts"
project_id = "test-project-511411"
topic_id = "demo-notification"

# Configure retries
custom_retry = retry.Retry(
    initial=1.0,
    maximum=10.0,
    multiplier=2.0,
    deadline=120.0,
    predicate=retry.if_exception_type(
        exceptions.DeadlineExceeded,
        exceptions.ServiceUnavailable,
        exceptions.InternalServerError,
        exceptions.ResourceExhausted,
    ),
)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

while True:
    try:
        # Fetch API data
        response = requests.get(api, timeout=30)
        response.raise_for_status()
        data = response.content

        # Publish with retry settings
        future = publisher.publish(
            topic_path,
            data,
            retry=custom_retry,
            timeout=120.0,
        )

        # Wait for the publish result
        message_id = future.result(timeout=150)

        print(f"Published successfully. Message ID: {message_id}")

    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")

    except Exception as e:
        print(f"Pub/Sub error: {type(e).__name__}: {e}")

    time.sleep(20)