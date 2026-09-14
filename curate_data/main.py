import io

import tempfile, os
import modelardb
# from minio import Minio

import upload_to_mrs

if __name__ == "__main__":
    # Connect to a cloud node.
    modelardbd_cloud = modelardb.connect("grpc://modelardb-cloud:9999")

    # Execute a query to retrieve data from the "wind" table.
    record_batch = modelardbd_cloud.read("SELECT * FROM wind LIMIT 100")
    df = record_batch.to_pandas()

    # Save the DataFrame to a CSV file in the MinIO object store.
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    # csv_buffer = io.BytesIO(csv_bytes)

    # call SLICES script
    # write bytes to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(csv_bytes)
        tmp_path = tmp.name

    try:
        upload_to_mrs.main(tmp_path)
    finally:
        os.unlink(tmp_path)
