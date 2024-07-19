import boto3
import io


def upload_to_bucket(file_like_obj: io.BytesIO, bucket: str, object_name: str) -> None:
    """Upload a file to an S3 bucket.
    Note: The file will be uploaded with public read access.

    Args:
        file_like_obj (io.BytesIO): File-like object to upload.
        bucket (str): Bucket to upload the file to.
        object_name (str): S3 object name.
    """
    s3_client = boto3.client('s3')
    with file_like_obj as f:
        s3_client.upload_fileobj(f, bucket, object_name, ExtraArgs={"ACL": "public-read"})
