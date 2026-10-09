import json
import os
import tempfile
import time

import modelardb

import upload_to_mrs
from metadata import download_metadata_from_minio

MODELARDB_URL = os.getenv("MODELARDB_URL", "grpc://modelardb-cloud:9999")
QUERY = os.getenv("MODELARDB_QUERY", "SELECT * FROM wind LIMIT 100")
METADATA_OBJECT = os.getenv("DATASET_ID", "wind-dataset.json")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "60"))
RETRY_DELAY_SECS = float(os.getenv("RETRY_DELAY_SECS", "5"))


def wait_for(description, fn):
    """Call fn() until it returns without raising, so the services we depend on can start up."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as error:
            print(f"[WAIT] {description} not ready ({attempt}/{MAX_ATTEMPTS}): {error}", flush=True)
            time.sleep(RETRY_DELAY_SECS)
    raise SystemExit(f"Gave up waiting for: {description}")


def fetch_dataframe():
    client = modelardb.connect(MODELARDB_URL)
    df = client.read(QUERY).to_pandas()
    if df.empty:
        # Data only reaches the cloud node after the edge nodes flush.
        raise RuntimeError("query returned 0 rows")
    return df


if __name__ == "__main__":
    # 1. Download the metadata JSON that the upload-metadata container put in MinIO.
    metadata = wait_for(
        f"metadata object {METADATA_OBJECT}",
        lambda: download_metadata_from_minio(METADATA_OBJECT),
    )
    print(f"[MINIO] Downloaded metadata '{metadata.get('identifier')}'", flush=True)
    
    # 2. Retrieve the dataset from ModelarDB and serialize it as CSV.
    df = wait_for("data in the wind table", fetch_dataframe)
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # 3. The downloaded metadata describes the full parquet file, but the file being uploaded
    #    is this CSV extract, so make the metadata match what is actually uploaded.
    metadata["byteSize"] = len(csv_bytes)
    metadata["format"] = "text/csv"
    metadata["dataStandard"] = "CSV"
    metadata["statistics"] = json.dumps({"records": {"count": len(df)}})

    # 4. Upload the dataset together with its metadata.
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(csv_bytes)
        tmp_path = tmp.name

    try:
        upload_to_mrs.main(tmp_path, metadata)
    finally:
        os.unlink(tmp_path)