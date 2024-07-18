import io
import json
import logging
import random
import time
from collections import namedtuple

from dlt_demo.utils.upload_data import upload_to_bucket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s [%(levelname)s] %(message)s")

Worker = namedtuple("Worker", ["id", "name"])

workers = [
    Worker(1, "John Doe"),
    Worker(2, "Jane Smith"),
    Worker(3, "Mike Johnson"),
    Worker(4, "Sarah Brown"),
    Worker(5, "David Wilson"),
]

addresses = ["New York", "Rio", "Tokyo", "London", "Paris"]
position = ["DE", "DS", "FE", "BE"]


def create_worker(*, new_field: bool = False) -> str:
    """Create a JSON object with random values. Optionally add a new field.

    Args:
        new_field (bool): Whether to add a new field to the object.

    Returns:
        str: JSON object as a string.
    """
    worker = {
        "worker_id": (person := random.choice(workers)).id,
        "worker_name": person.name,
        "salary": random.randint(1000, 10000),
        "bonus": random.randint(0, 1000),
        "address": random.choice(addresses),
        "created_at": int(time.time()),
        "gender": random.choice(["M", "F", "O"]),
    }
    if new_field and random.random() <= 0.1:
        worker |= {"position": random.choice(position)}

    return json.dumps(worker).encode("utf-8")


def create_file_like_object(json_str: str) -> io.BytesIO:
    """Create a file-like object from a JSON string.

    Args:
        json_str (str): JSON string to convert to a file-like object.

    Returns:
        io.BytesIO: File-like object that can be uploaded to S3.
    """
    return io.BytesIO(json_str)


def stream_mock_data(*, delay: int) -> None:
    """Stream mock data to S3 bucket.

    Args:
        delay (int): Delay between each upload in seconds.
    """
    uploaded_files = 0
    while True:
        worker_obj = create_file_like_object(create_worker(new_field=True))
        upload_to_bucket(worker_obj, "databricks-dlt-demo-dev", f"mock_data/worker-{int(time.time())}.json")
        uploaded_files += 1
        logger.info(f"[{uploaded_files}] - Uploaded mock data to S3.")
        time.sleep(delay)


if __name__ == '__main__':
    stream_mock_data(delay=1)
