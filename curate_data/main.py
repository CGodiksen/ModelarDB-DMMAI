import io

import modelardb
from minio import Minio

if __name__ == "__main__":
    # Connect to a cloud node.
    modelardbd_cloud = modelardb.connect("grpc://localhost:9999")

    # Execute a query to retrieve data from the "wind" table.
    record_batch = modelardbd_cloud.read("SELECT * FROM wind LIMIT 100")
    df = record_batch.to_pandas()

    # Save the DataFrame to a CSV file in the MinIO object store.
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_buffer = io.BytesIO(csv_bytes)

    client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", region="eu-central-1",
                   secure=False)
    client.put_object(bucket_name="datasets", object_name="wind.csv", length=len(csv_bytes), data=csv_buffer)
