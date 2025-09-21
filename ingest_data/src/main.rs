mod util;

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use modelardb_embedded::TableType;
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::client::{Client, Node};
use tokio::time;

const TABLE_NAME: &str = "wind";

/// Create the wind table in the given ModelarDB client.
async fn create_table(mut modelardb_client: Client) {
    let schema = util::table_schema();
    let table_type = TableType::TimeSeriesTable(schema, HashMap::new(), HashMap::new());

    modelardb_client
        .create(TABLE_NAME, table_type)
        .await
        .unwrap();
}

/// Ingest the given wind data into the given ModelarDB client in an infinite loop by generating
/// time series data with the given park ID and windmill ID in batches of 1000 rows every 2 seconds.
async fn ingest_into_table_task(
    mut modelardb_client: Client,
    wind_data: RecordBatch,
    park_id: &str,
    windmill_id: &str,
) {
    let num_rows = wind_data.num_rows();
    let batch_size = 1000;
    let mut start = 0;

    loop {
        let end = if start + batch_size > num_rows {
            num_rows
        } else {
            start + batch_size
        };

        let slice = wind_data.slice(start, end - start);
        let time_series = util::generate_time_series_data(slice, park_id, windmill_id).await;

        modelardb_client
            .write(TABLE_NAME, time_series)
            .await
            .unwrap();

        start = end % num_rows;

        time::sleep(time::Duration::from_secs(2)).await;
    }
}

#[tokio::main]
async fn main() {
    // Connect to the two ModelarDB edge nodes and create the table.
    let edge_1 = Node::Server("grpc://host.docker.internal:9991".to_owned());
    let edge_1_client = Client::connect(edge_1).await.unwrap();
    create_table(edge_1_client.clone()).await;

    let edge_2 = Node::Server("grpc://host.docker.internal:9992".to_owned());
    let edge_2_client = Client::connect(edge_2).await.unwrap();
    create_table(edge_2_client.clone()).await;

    let wind_data = util::read_wind_data().await;

    // Start two tasks that ingest data into the two edge nodes.
    tokio::spawn(ingest_into_table_task(
        edge_1_client,
        wind_data.clone(),
        "park_1",
        "windmill_1",
    ));

    tokio::spawn(ingest_into_table_task(
        edge_2_client,
        wind_data,
        "park_1",
        "windmill_2",
    ));
}
