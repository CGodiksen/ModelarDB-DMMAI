mod util;

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use modelardb_embedded::TableType;
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::client::{Client, Node};
use tokio::time;

const TABLE_NAME: &str = "wind";
const INGESTION_INTERVAL_SECS: u64 = 2;
const FLUSH_INTERVAL_SECS: u64 = 10;

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
/// time series data with the given park ID and windmill ID in batches of 1000 rows every
/// [`INGESTION_INTERVAL_SECS`] seconds.
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

        time::sleep(time::Duration::from_secs(INGESTION_INTERVAL_SECS)).await;
    }
}

/// Periodically flush and vacuum the given ModelarDB client every [`FLUSH_INTERVAL_SECS`] seconds.
async fn flush_and_vacuum_task(modelardb_node: Node) {
    loop {
        // Flush all data from the node to the remote object store.
        let mut flight_client = FlightServiceClient::connect(modelardb_node.url().to_owned())
            .await
            .unwrap();

        let action = Action {
            r#type: "FlushNode".to_owned(),
            body: vec![].into(),
        };

        flight_client.do_action(action.clone()).await.unwrap();

        // Vacuum the node to remove any deleted data.
        flight_client
            .do_get(Ticket::new("VACUUM RETAIN 0".to_owned()))
            .await
            .unwrap();

        time::sleep(time::Duration::from_secs(FLUSH_INTERVAL_SECS)).await;
    }
}

#[tokio::main]
async fn main() {
    // Connect to the two ModelarDB edge nodes.
    let edge_1_node = Node::Server("grpc://modelardb-edge-1:9991".to_owned());
    let edge_1_client = Client::connect(edge_1_node.clone()).await.unwrap();

    let edge_2_node = Node::Server("grpc://modelardb-edge-2:9992".to_owned());
    let edge_2_client = Client::connect(edge_2_node.clone()).await.unwrap();

    // Create the table on one of the nodes. This creates the table on every node in the ModelarDB
    // cluster, specifically the two edge nodes and the cloud node.
    create_table(edge_1_client.clone()).await;

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

    // Start two tasks that periodically flush and vacuum the two edge nodes.
    tokio::spawn(flush_and_vacuum_task(edge_1_node));
    tokio::spawn(flush_and_vacuum_task(edge_2_node));

    // Wait for the CTRL+C signal to exit.
    tokio::signal::ctrl_c().await.unwrap();
}
