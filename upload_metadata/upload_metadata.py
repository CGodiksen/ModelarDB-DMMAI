"""Upload the wind dataset metadata JSON to MinIO (S3-compatible).

Python port of `upload_metadata_to_minio` from the Rust ingest service. All configuration comes
from environment variables, so it shares the `aws-variables` block from the compose file.
boto3 reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION by itself.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# AWS_ENDPOINT, AWS_DEFAULT_REGION are variable that ModelarDB searches for the object stores
DATASET_ID = os.environ.get("DATASET_ID", "wind-dataset")
DATASET_FILE = os.environ.get("DATASET_FILE", "/data/wind_cleaned.parquet")
MINIO_METADATA_BUCKET = os.environ.get("METADATA_BUCKET", "mrsmetadata")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT") or os.environ.get("AWS_ENDPOINT") or "http://minio:9000"
REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")
MAX_ATTEMPTS = int(os.environ.get("UPLOAD_MAX_ATTEMPTS", "30"))
RETRY_DELAY_SECS = float(os.environ.get("UPLOAD_RETRY_DELAY_SECS", "2"))


def dataset_stats() -> tuple[int, int]:
    """Return (record_count, file_size_in_bytes). RECORD_COUNT overrides the parquet row count."""
    file_size = os.path.getsize(DATASET_FILE)
    if os.environ.get("RECORD_COUNT"):
        return int(os.environ["RECORD_COUNT"]), file_size
    # Only the parquet footer is read, so this is cheap even for large files.
    return pq.ParquetFile(DATASET_FILE).metadata.num_rows, file_size


def build_metadata(record_count: int, file_size: int) -> dict:
    created_at = (
        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )
    return {
        "$type": "Dataset",
        "identifier": DATASET_ID,
        "alternateIdentifier": "wind-cleaned-parquet",
        "downloadUrl": None,
        "systemCreationDate": None,
        "internalIdentifier": None,
        "paymentModel": "http://something-testtesttest",
        "pricing": None,
        "scientificDomains": ["wind energy"],
        "scientificSubdomains": ["wind turbines"],
        "address": "",
        "dateTimeStart": None,
        "dateTimeEnd": None,
        "locations": [],
        "byteSize": file_size,
        "duration": None,
        "format": "application/parquet",
        "medium": "",
        "compressionFormat": "none",
        "fileMetadata": "ModelarDB wind dataset",
        "dataStandard": "Parquet",
        "encodingFormat": "UTF-8",
        "creators": [],
        "name": "Wind Dataset",
        "description": "Wind turbine data ingested into ModelarDB.",
        "resourceType": "Dataset",
        "subjects": [],
        "keywords": ["wind", "ModelarDB"],
        "createdAt": created_at,
        "version": "V1",
        "metadataProfile": "Slices_V0_2",
        "contributors": [],
        "accessType": "Physical",
        "accessMode": "FreeConditionally",
        "requiredObjects": [],
        "relatedObjects": [],
        "primaryLanguages": ["en"],
        "otherLanguages": [],
        "contact": None,
        "publicContact": None,
        "rights": "",
        "rightsUri": "",
        "accessRights": "Restricted",
        "license": "MIT",
        "licenseUri": "https://opensource.org/licenses/MIT",
        "copyrightsHolder": "SLICES-RI",
        "confidentialityDeclaration": "",
        "specialPermissions": "",
        "restrictions": "",
        "citationRequirements": "",
        "conditions": "",
        "disclaimer": "",
        "provenance": "ModelarDB ingestion",
        # The schema expects statistics as a JSON-encoded string, not a nested object.
        "statistics": json.dumps({"records": {"count": '1000'}}),
    }


def main() -> int:
    print("[MINIO] Starting metadata upload", flush=True)

    record_count, file_size = dataset_stats()
    object_name = f"{DATASET_ID}.json"
    payload = json.dumps(build_metadata(record_count, file_size), indent=2).encode("utf-8")

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        region_name=REGION,
        config=Config(
            s3={"addressing_style": "path"},  # http://minio:9000/bucket/key
            retries={"max_attempts": 1},  # retries are handled by the loop below
            connect_timeout=5,
        ),
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            client.put_object(
                Bucket=MINIO_METADATA_BUCKET,
                Key=object_name,
                Body=payload,
                ContentType="application/json",
            )
            print(f"[MINIO] Metadata uploaded to s3://{MINIO_METADATA_BUCKET}/{object_name}", flush=True)
            return 0
        except (BotoCoreError, ClientError) as error:
            print(
                f"[MINIO] Attempt {attempt}/{MAX_ATTEMPTS} failed for {MINIO_ENDPOINT} "
                f"(bucket={MINIO_METADATA_BUCKET}, object={object_name}): {error}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(RETRY_DELAY_SECS)

    print("[MINIO] Giving up on metadata upload", file=sys.stderr, flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())