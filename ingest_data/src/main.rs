mod util;

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use modelardb_embedded::TableType;
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::client::{Client, Node};

async fn create_table(mut modelardb_client: Client) {
    let schema = util::table_schema();
    let table_type = TableType::TimeSeriesTable(schema, HashMap::new(), HashMap::new());

    modelardb_client.create("wind", table_type).await.unwrap();
}

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

        start = end % num_rows;
    }
}

#[tokio::main]
async fn main() {
    // Connect to the two ModelarDB edge nodes and create the table.
    let edge_1 = Node::Server("grpc://host.docker.internal:9991".to_owned());
    let edge_1_client = Client::connect(edge_1).await.unwrap();
    create_table(edge_1_client).await;

    let edge_2 = Node::Server("grpc://host.docker.internal:9992".to_owned());
    let edge_2_client = Client::connect(edge_2).await.unwrap();
    create_table(edge_2_client).await;

    let wind_data = util::read_wind_data().await;
}
