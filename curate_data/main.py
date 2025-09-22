from pathlib import Path

import modelardb

if __name__ == "__main__":
    # Connect to a cloud node.
    modelardbd_cloud = modelardb.connect(modelardb.Server("grpc://host.docker.internal:9999"))

    # Execute a query to retrieve data from the "wind" table.
    record_batch = modelardbd_cloud.read("SELECT * FROM wind LIMIT 100")
    df = record_batch.to_pandas()

    # Save the DataFrame to a CSV file.
    output_file = "wind.csv"
    output_dir = Path("datasets")

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / output_file, index=False)
