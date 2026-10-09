import json
import os
from typing import Any, Dict

from minio import Minio


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    return Minio(
        endpoint.removeprefix("http://").removeprefix("https://"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
        secret_key=os.getenv(
            "MINIO_SECRET_KEY", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"
        ),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def download_metadata_from_minio(object_name: str) -> Dict[str, Any]:
    bucket = os.getenv("MINIO_METADATA_BUCKET", "datasets")
    client = create_minio_client()
    response = client.get_object(bucket, object_name)
    try:
        metadata = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()

    if not isinstance(metadata, dict):
        raise ValueError(
            f"Metadata object s3://{bucket}/{object_name} is not a JSON object"
        )
    return metadata
